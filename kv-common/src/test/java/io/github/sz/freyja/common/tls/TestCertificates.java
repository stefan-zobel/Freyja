package io.github.sz.freyja.common.tls;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Makes certificate authorities and certificates for tests, with the {@code keytool} of the
 * JDK that runs the test. Everything is a PKCS12 file with the password {@link #PASSWORD}.
 */
public final class TestCertificates {

    public static final String PASSWORD = "changeit";

    /**
     * @param store      the key of the authority; never given to anything under test
     * @param trustStore a store with nothing but the certificate of the authority
     */
    public record Authority(String name, Path store, Path certificate, Path trustStore) {
    }

    private final Path directory;
    private final Path passwordFile;

    public TestCertificates(Path directory) throws IOException {
        this.directory = directory;
        Files.createDirectories(directory);
        this.passwordFile = directory.resolve("password");
        Files.writeString(passwordFile, PASSWORD + "\n", StandardCharsets.UTF_8);
    }

    public Path passwordFile() {
        return passwordFile;
    }

    /** A new authority that signs its own certificate. */
    public Authority authority(String name) throws IOException {
        Path store = directory.resolve("ca-" + name + ".p12");
        Path certificate = directory.resolve("ca-" + name + ".pem");
        Path trust = directory.resolve("trust-" + name + ".p12");
        keytool("-genkeypair", "-alias", "ca", "-keyalg", "EC", "-groupname", "secp256r1", "-dname",
                "CN=Test Authority " + name, "-ext", "bc:c=ca:true", "-ext", "ku:c=keyCertSign,cRLSign",
                "-validity", "30", "-keystore", store.toString());
        keytool("-exportcert", "-alias", "ca", "-rfc", "-file", certificate.toString(), "-keystore",
                store.toString());
        keytool("-importcert", "-alias", "ca", "-file", certificate.toString(), "-noprompt", "-keystore",
                trust.toString());
        return new Authority(name, store, certificate, trust);
    }

    /**
     * A key store with a new key and a certificate for it, signed by {@code authority}.
     *
     * @param subject the subject of the certificate, for example {@code CN=node 1,OU=node}
     * @param names   what the certificate is made out for, for example
     *                {@code ip:127.0.0.1,dns:localhost}; {@code null} for nothing
     */
    public Path issue(Authority authority, String file, String subject, String names) throws IOException {
        Path store = directory.resolve(file + ".p12");
        Path request = directory.resolve(file + ".csr");
        Path certificate = directory.resolve(file + ".pem");
        Files.deleteIfExists(store);
        keytool("-genkeypair", "-alias", "key", "-keyalg", "EC", "-groupname", "secp256r1", "-dname", subject,
                "-validity", "30", "-keystore", store.toString());
        keytool("-certreq", "-alias", "key", "-file", request.toString(), "-keystore", store.toString());
        List<String> sign = new ArrayList<>(List.of("-gencert", "-alias", "ca", "-infile", request.toString(),
                "-outfile", certificate.toString(), "-rfc", "-validity", "30", "-ext", "ku:c=digitalSignature",
                "-ext", "eku=serverAuth,clientAuth"));
        if (names != null) {
            sign.add("-ext");
            sign.add("san=" + names);
        }
        sign.add("-keystore");
        sign.add(authority.store().toString());
        keytool(sign.toArray(String[]::new));
        // The reply is taken only if its chain ends in a certificate the store knows.
        keytool("-importcert", "-alias", "ca", "-file", authority.certificate().toString(), "-noprompt",
                "-keystore", store.toString());
        keytool("-importcert", "-alias", "key", "-file", certificate.toString(), "-noprompt", "-keystore",
                store.toString());
        return store;
    }

    private void keytool(String... arguments) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        command.addAll(List.of(arguments));
        command.addAll(List.of("-storetype", "PKCS12", "-storepass", PASSWORD));
        Path output = directory.resolve("keytool.out");
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile())
                .start();
        try {
            if (!process.waitFor(120, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new IOException("keytool does not end: " + command);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while keytool ran", e);
        }
        if (process.exitValue() != 0) {
            throw new IOException("keytool failed: " + command + "\n" + Files.readString(output));
        }
    }
}
