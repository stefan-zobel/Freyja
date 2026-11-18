package io.github.sz.freyja.node.net;

import java.io.IOException;
import java.nio.file.Path;

import io.github.sz.freyja.common.tls.Tls;

/**
 * How a node protects its three listeners (docs/spec/operations.md, section 5).
 *
 * <p>The node shows the same certificate everywhere. Whom it accepts is said per listener: by
 * the authorities it trusts there and, optionally, by a name the certificate of the other
 * side must carry. Clients may prove themselves with a token instead of a certificate.
 *
 * @param peers          TLS between nodes, in both directions; {@code null} in insecure mode
 * @param clients        TLS of the listener for clients
 * @param administrators TLS of the listener for administrators
 * @param tokens         the tokens of clients; {@code null} if clients need a certificate
 */
public record Security(Tls peers, Tls clients, Tls administrators, TokenFile tokens) {

    /**
     * No TLS and no authentication: for development and tests only. Whoever reaches a port
     * of the node is taken for what it says it is.
     */
    public static final Security INSECURE = new Security(null, null, null, null);

    /**
     * The files of a secured node.
     *
     * @param keyStore     the private key of the node and its certificate chain (PKCS12)
     * @param passwordFile the password of all stores in its first line; {@code null} for the
     *                     empty password
     * @param peerTrust    the authorities of the certificates of nodes
     * @param peerName     the name a certificate of a node must carry, or {@code null}
     * @param clientTrust  the authorities of the certificates of clients; {@code null} if
     *                     clients prove themselves with tokens only
     * @param clientName   the name a certificate of a client must carry, or {@code null}
     * @param clientTokens the token file; {@code null} if clients need a certificate
     * @param adminTrust   the authorities of the certificates of administrators
     * @param adminName    the name a certificate of an administrator must carry, or {@code null}
     */
    public record Files(Path keyStore, Path passwordFile, Path peerTrust, String peerName, Path clientTrust,
                        String clientName, Path clientTokens, Path adminTrust, String adminName) {
        public Files {
            if (keyStore == null || peerTrust == null || adminTrust == null) {
                throw new IllegalArgumentException(
                        "a secured node needs its key store and the trust stores for peers and administrators");
            }
            if (clientTrust == null && clientTokens == null) {
                throw new IllegalArgumentException(
                        "a secured node needs a trust store or a token file for clients, or both");
            }
            if (clientTrust == null && clientName != null) {
                throw new IllegalArgumentException("a name for clients without a trust store for clients");
            }
        }
    }

    /**
     * Reads the files of a secured node.
     *
     * @throws IOException if a file cannot be read or cannot be used
     */
    public static Security open(Files files) throws IOException {
        return new Security(
                Tls.open(new Tls.Settings(files.keyStore(), files.peerTrust(), files.passwordFile(), files.peerName())),
                Tls.open(new Tls.Settings(files.keyStore(), files.clientTrust(), files.passwordFile(),
                        files.clientName())),
                Tls.open(new Tls.Settings(files.keyStore(), files.adminTrust(), files.passwordFile(),
                        files.adminName())),
                files.clientTokens() == null ? null : TokenFile.open(files.clientTokens()));
    }

    public boolean insecure() {
        return peers == null;
    }

    /** How often changed certificates or tokens were taken into use. */
    public long reloadCount() {
        return insecure() ? 0 : peers.reloadCount() + clients.reloadCount() + administrators.reloadCount()
                + (tokens == null ? 0 : tokens.reloadCount());
    }

    /** How often changed certificates or tokens could not be used. */
    public long reloadFailureCount() {
        return insecure() ? 0 : peers.reloadFailureCount() + clients.reloadFailureCount()
                + administrators.reloadFailureCount() + (tokens == null ? 0 : tokens.reloadFailureCount());
    }
}
