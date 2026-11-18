package io.github.sz.freyja.common.tls;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/**
 * TLS for one kind of connection (docs/spec/operations.md, section 5): the certificate this
 * side shows, the authorities whose certificates it accepts from the other side, and
 * optionally a name the certificate of the other side must carry. Only the TLS implementation
 * of the JDK is used, and only TLS 1.3.
 *
 * <p>The files are read again when their content has changed: a connection that is made after
 * a change uses the new files, and connections that exist go on as they are. Files that
 * cannot be used leave the last good ones in force.
 */
public final class Tls {

    /** What a side that accepts a connection asks of the side that connects. */
    public enum ClientCertificate {
        /** None is asked for. */
        NONE,
        /** One is asked for; a connection without one is accepted and is not certified. */
        WANTED,
        /** A connection without one is refused. */
        NEEDED
    }

    /**
     * @param keyStore     a PKCS12 file with the private key of this side and its certificate
     *                     chain; {@code null} for a side that shows no certificate
     * @param trustStore   a PKCS12 file with the certificates of the authorities that are
     *                     trusted; {@code null} for a side that accepts no certificate
     * @param passwordFile a file whose first line is the password of both stores;
     *                     {@code null} for the empty password
     * @param requiredName {@code ATTRIBUTE=VALUE}, for example {@code OU=freyja-admin}: the
     *                     subject of the certificate of the other side must have this
     *                     attribute with this value; {@code null} for no such demand
     */
    public record Settings(Path keyStore, Path trustStore, Path passwordFile, String requiredName) {
        public Settings {
            if (keyStore == null && trustStore == null) {
                throw new IllegalArgumentException("neither a key store nor a trust store");
            }
            if (requiredName != null) {
                parseName(requiredName);
            }
        }
    }

    private static final System.Logger LOG = System.getLogger("freyja.tls");
    private static final String[] PROTOCOLS = {"TLSv1.3"};
    private static final char[] NO_PASSWORD = new char[0];

    private final Settings settings;
    private final String nameType;
    private final String nameValue;
    private final long recheckNanos;
    private final AtomicLong reloads = new AtomicLong();
    private final AtomicLong reloadFailures = new AtomicLong();
    private SSLContext context;
    private byte[] keyBytes;
    private byte[] trustBytes;
    private byte[] passwordBytes;
    private long checkedNanos;

    private Tls(Settings settings, long recheckMillis) throws IOException {
        this.settings = settings;
        if (settings.requiredName() == null) {
            nameType = null;
            nameValue = null;
        } else {
            String[] name = parseName(settings.requiredName());
            nameType = name[0];
            nameValue = name[1];
        }
        this.recheckNanos = recheckMillis * 1_000_000L;
        load();
        checkedNanos = System.nanoTime();
    }

    /**
     * Reads the files. They are looked at again at most once a second.
     *
     * @throws IOException if a file cannot be read or cannot be used
     */
    public static Tls open(Settings settings) throws IOException {
        return open(settings, 1_000);
    }

    /** As {@link #open(Settings)}, with the time after which the files are looked at again. */
    public static Tls open(Settings settings, long recheckMillis) throws IOException {
        return new Tls(Objects.requireNonNull(settings, "settings"), recheckMillis);
    }

    public Settings settings() {
        return settings;
    }

    /** How often changed files were taken into use. */
    public long reloadCount() {
        return reloads.get();
    }

    /** How often changed files could not be used and the last good ones stayed in force. */
    public long reloadFailureCount() {
        return reloadFailures.get();
    }

    /**
     * Starts TLS on a connection this side has made, and checks the other side: its
     * certificate must come from a trusted authority, must be made out for {@code host}, and
     * must carry the required name.
     *
     * @param connected the connection; a time limit set on it bounds the TLS handshake
     * @param host      the name or the address that was connected to
     * @return the connection to use from now on; closing it closes {@code connected}
     * @throws IOException if the handshake fails or the other side is not accepted; the
     *                     connection is left to the caller to close
     */
    public SSLSocket connect(Socket connected, String host, int port) throws IOException {
        if (settings.trustStore() == null) {
            throw new IOException("without a trust store no node can be recognized");
        }
        SSLSocket tls = (SSLSocket) current().getSocketFactory().createSocket(connected, host, port, true);
        SSLParameters parameters = tls.getSSLParameters();
        parameters.setProtocols(PROTOCOLS);
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        tls.setSSLParameters(parameters);
        tls.startHandshake();
        checkName(tls);
        return tls;
    }

    /**
     * Starts TLS on a connection this side has accepted. A certificate of the other side
     * must come from a trusted authority and must carry the required name.
     *
     * @param accepted the connection; a time limit set on it bounds the TLS handshake
     * @return the connection to use from now on; closing it closes {@code accepted}
     * @throws IOException if the handshake fails or the other side is not accepted; the
     *                     connection is left to the caller to close
     */
    public SSLSocket accept(Socket accepted, ClientCertificate certificate) throws IOException {
        if (settings.keyStore() == null) {
            throw new IOException("without a key store no connection can be accepted");
        }
        SSLSocket tls = (SSLSocket) current().getSocketFactory().createSocket(accepted, null, true);
        SSLParameters parameters = tls.getSSLParameters();
        parameters.setProtocols(PROTOCOLS);
        switch (certificate) {
            case NEEDED -> parameters.setNeedClientAuth(true);
            case WANTED -> parameters.setWantClientAuth(true);
            case NONE -> {
                // nothing is asked for
            }
        }
        tls.setSSLParameters(parameters);
        tls.startHandshake();
        if (certified(tls)) {
            checkName(tls);
        } else if (certificate == ClientCertificate.NEEDED) {
            throw new IOException("the other side showed no certificate");
        }
        return tls;
    }

    /** Whether the other side of a connection showed a certificate that was accepted. */
    public static boolean certified(SSLSocket connection) {
        try {
            return connection.getSession().getPeerCertificates().length > 0;
        } catch (SSLPeerUnverifiedException e) {
            return false;
        }
    }

    private void checkName(SSLSocket connection) throws IOException {
        if (nameType == null) {
            return;
        }
        Certificate[] chain = connection.getSession().getPeerCertificates();
        String subject = ((X509Certificate) chain[0]).getSubjectX500Principal().getName();
        try {
            for (Rdn rdn : new LdapName(subject).getRdns()) {
                if (rdn.size() == 1 && rdn.getType().equalsIgnoreCase(nameType)
                        && nameValue.equals(String.valueOf(rdn.getValue()))) {
                    return;
                }
            }
        } catch (InvalidNameException e) {
            // a subject that cannot be read does not carry the name
        }
        throw new IOException("the certificate of the other side does not carry " + settings.requiredName());
    }

    private static String[] parseName(String name) {
        int equals = name.indexOf('=');
        if (equals <= 0 || equals == name.length() - 1) {
            throw new IllegalArgumentException("not ATTRIBUTE=VALUE: " + name);
        }
        return new String[] {name.substring(0, equals).trim(), name.substring(equals + 1).trim()};
    }

    /** The context for a new connection: of the files as they are now, if they can be used. */
    private synchronized SSLContext current() {
        long now = System.nanoTime();
        if (now - checkedNanos >= recheckNanos) {
            checkedNanos = now;
            try {
                if (changed()) {
                    load();
                    reloads.incrementAndGet();
                    LOG.log(System.Logger.Level.INFO, "the certificates were read again: " + describe());
                }
            } catch (IOException e) {
                reloadFailures.incrementAndGet();
                LOG.log(System.Logger.Level.WARNING, "the changed certificates cannot be used, the last good ones"
                        + " stay in force (" + describe() + "): " + e.getMessage());
            }
        }
        return context;
    }

    private String describe() {
        return "key store " + settings.keyStore() + ", trust store " + settings.trustStore();
    }

    private static byte[] read(Path file) throws IOException {
        return file == null ? null : Files.readAllBytes(file);
    }

    private boolean changed() throws IOException {
        return !Arrays.equals(keyBytes, read(settings.keyStore()))
                || !Arrays.equals(trustBytes, read(settings.trustStore()))
                || !Arrays.equals(passwordBytes, read(settings.passwordFile()));
    }

    private void load() throws IOException {
        byte[] key = read(settings.keyStore());
        byte[] trust = read(settings.trustStore());
        byte[] secret = read(settings.passwordFile());
        char[] password = NO_PASSWORD;
        if (secret != null) {
            String text = new String(secret, StandardCharsets.UTF_8);
            int end = 0;
            while (end < text.length() && text.charAt(end) != '\n' && text.charAt(end) != '\r') {
                end++;
            }
            password = text.substring(0, end).toCharArray();
        }
        try {
            KeyManager[] keys = new KeyManager[0];
            if (key != null) {
                KeyStore store = store(key, password, settings.keyStore());
                boolean hasKey = false;
                for (String alias : Collections.list(store.aliases())) {
                    hasKey |= store.isKeyEntry(alias);
                }
                if (!hasKey) {
                    throw new IOException(settings.keyStore() + " holds no private key");
                }
                KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                factory.init(store, password);
                keys = factory.getKeyManagers();
            }
            // Without a trust store nothing is trusted: never the authorities the JDK ships.
            TrustManager[] trusted = new TrustManager[0];
            if (trust != null) {
                KeyStore store = store(trust, password, settings.trustStore());
                boolean hasCertificate = false;
                for (String alias : Collections.list(store.aliases())) {
                    hasCertificate |= store.isCertificateEntry(alias);
                }
                if (!hasCertificate) {
                    throw new IOException(settings.trustStore() + " holds no certificate of an authority");
                }
                TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                factory.init(store);
                trusted = factory.getTrustManagers();
            }
            SSLContext fresh = SSLContext.getInstance("TLSv1.3");
            fresh.init(keys, trusted, null);
            context = fresh;
            keyBytes = key;
            trustBytes = trust;
            passwordBytes = secret;
        } catch (GeneralSecurityException e) {
            throw new IOException("the certificates cannot be used: " + e.getMessage(), e);
        } finally {
            Arrays.fill(password, '\0');
        }
    }

    private static KeyStore store(byte[] bytes, char[] password, Path file) throws IOException {
        try {
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(new ByteArrayInputStream(bytes), password);
            return store;
        } catch (GeneralSecurityException | IOException e) {
            throw new IOException(file + " is not a PKCS12 store that opens with the password: " + e.getMessage(), e);
        }
    }
}
