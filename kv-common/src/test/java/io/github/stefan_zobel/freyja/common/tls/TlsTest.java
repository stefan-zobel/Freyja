package io.github.stefan_zobel.freyja.common.tls;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSocket;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.github.stefan_zobel.freyja.common.tls.Tls.ClientCertificate;

/** TLS as the three listeners of a node use it (docs/spec/operations.md, section 5). */
class TlsTest {

    private static final String CERTIFIED = "certified";
    private static final String PLAIN = "plain";
    private static final String FAILED = "failed";

    @TempDir
    static Path directory;

    private static TestCertificates certificates;
    private static TestCertificates.Authority ours;
    private static TestCertificates.Authority theirs;
    private static Path node;
    private static Path administrator;
    private static Path client;
    private static Path stranger;

    @BeforeAll
    static void makeCertificates() throws IOException {
        certificates = new TestCertificates(directory);
        ours = certificates.authority("ours");
        theirs = certificates.authority("theirs");
        node = certificates.issue(ours, "node", "CN=node 1,OU=node,O=Test", "ip:127.0.0.1");
        administrator = certificates.issue(ours, "administrator", "CN=alice,OU=admin,O=Test", null);
        client = certificates.issue(ours, "client", "CN=app,OU=client,O=Test", null);
        stranger = certificates.issue(theirs, "stranger", "CN=mallory,OU=admin,O=Test", "ip:127.0.0.1");
    }

    private static Tls tls(Path keyStore, Path trustStore, String name) throws IOException {
        return Tls.open(new Tls.Settings(keyStore, trustStore, certificates.passwordFile(), name), 0);
    }

    /**
     * One connection: the client says one byte, the server answers whether the client is
     * certified for it.
     *
     * @return what the client learns: {@link #CERTIFIED}, {@link #PLAIN}, or {@link #FAILED}
     *         if no answer came
     */
    private static String talk(Tls server, ClientCertificate asked, Tls client, String host) throws Exception {
        try (ServerSocket listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> serving = CompletableFuture.runAsync(() -> {
                try (Socket raw = listener.accept()) {
                    raw.setSoTimeout(10_000);
                    SSLSocket secured = server.accept(raw, asked);
                    if (secured.getInputStream().read() == 7) {
                        secured.getOutputStream().write(Tls.certified(secured) ? 1 : 2);
                        secured.getOutputStream().flush();
                        secured.getInputStream().read(); // until the client closes
                    }
                } catch (IOException e) {
                    // the connection was not accepted; the client learns it by silence
                }
            });
            String outcome;
            try (Socket raw = new Socket()) {
                raw.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), listener.getLocalPort()), 5_000);
                raw.setSoTimeout(10_000);
                SSLSocket secured = client.connect(raw, host, listener.getLocalPort());
                secured.getOutputStream().write(7);
                secured.getOutputStream().flush();
                int answer = secured.getInputStream().read();
                outcome = answer == 1 ? CERTIFIED : answer == 2 ? PLAIN : FAILED;
            } catch (IOException e) {
                outcome = FAILED;
            }
            serving.get(30, TimeUnit.SECONDS);
            return outcome;
        }
    }

    @Test
    void bothSidesShowCertificatesOfTheSameAuthority() throws Exception {
        assertEquals(CERTIFIED, talk(tls(node, ours.trustStore(), null), ClientCertificate.NEEDED,
                tls(client, ours.trustStore(), null), "127.0.0.1"));
    }

    @Test
    void aSideWithoutACertificateIsTakenOnlyWhereNoneIsNeeded() throws Exception {
        Tls server = tls(node, ours.trustStore(), null);
        Tls bare = tls(null, ours.trustStore(), null);
        assertEquals(FAILED, talk(server, ClientCertificate.NEEDED, bare, "127.0.0.1"));
        assertEquals(PLAIN, talk(server, ClientCertificate.WANTED, bare, "127.0.0.1"));
        assertEquals(PLAIN, talk(server, ClientCertificate.NONE, bare, "127.0.0.1"));
        assertEquals(CERTIFIED, talk(server, ClientCertificate.WANTED, tls(client, ours.trustStore(), null),
                "127.0.0.1"));
        // A listener without a trust store takes no certificate at all, and none of the
        // authorities that come with the JDK.
        assertEquals(PLAIN, talk(tls(node, null, null), ClientCertificate.NONE, bare, "127.0.0.1"));
    }

    @Test
    void aCertificateOfAnotherAuthorityIsRefusedOnEitherSide() throws Exception {
        // The client is not of our authority.
        assertEquals(FAILED, talk(tls(node, ours.trustStore(), null), ClientCertificate.NEEDED,
                tls(stranger, ours.trustStore(), null), "127.0.0.1"));
        // Where a certificate is only wanted, the listener says whose it takes, and the
        // client shows none: the connection counts as one without a certificate.
        assertEquals(PLAIN, talk(tls(node, ours.trustStore(), null), ClientCertificate.WANTED,
                tls(stranger, ours.trustStore(), null), "127.0.0.1"));
        // The node is not of the authority the client trusts.
        assertEquals(FAILED, talk(tls(stranger, ours.trustStore(), null), ClientCertificate.NEEDED,
                tls(client, ours.trustStore(), null), "127.0.0.1"));
    }

    @Test
    void theCertificateOfANodeMustBeMadeOutForTheAddressThatWasDialed() throws Exception {
        Tls server = tls(node, ours.trustStore(), null);
        Tls caller = tls(client, ours.trustStore(), null);
        assertEquals(FAILED, talk(server, ClientCertificate.NEEDED, caller, "localhost"));
        assertEquals(FAILED, talk(server, ClientCertificate.NEEDED, caller, "127.0.0.2"));
        // A certificate that is made out for nothing, as that of a client, is no node.
        assertEquals(FAILED, talk(tls(client, ours.trustStore(), null), ClientCertificate.NEEDED, caller,
                "127.0.0.1"));
    }

    @Test
    void aRequiredNameKeepsTheRolesApartUnderOneAuthority() throws Exception {
        Tls forAdministrators = tls(node, ours.trustStore(), "OU=admin");
        assertEquals(CERTIFIED, talk(forAdministrators, ClientCertificate.NEEDED,
                tls(administrator, ours.trustStore(), null), "127.0.0.1"));
        assertEquals(FAILED, talk(forAdministrators, ClientCertificate.NEEDED,
                tls(client, ours.trustStore(), null), "127.0.0.1"));
        // A certificate that is shown where one is only wanted must carry the name too.
        assertEquals(FAILED, talk(forAdministrators, ClientCertificate.WANTED,
                tls(client, ours.trustStore(), null), "127.0.0.1"));
        // The attribute is found whatever its case; the value must be the same.
        assertEquals(CERTIFIED, talk(tls(node, ours.trustStore(), "ou=admin"), ClientCertificate.NEEDED,
                tls(administrator, ours.trustStore(), null), "127.0.0.1"));
        assertEquals(FAILED, talk(tls(node, ours.trustStore(), "OU=Admin"), ClientCertificate.NEEDED,
                tls(administrator, ours.trustStore(), null), "127.0.0.1"));
        assertEquals(FAILED, talk(tls(node, ours.trustStore(), "CN=admin"), ClientCertificate.NEEDED,
                tls(administrator, ours.trustStore(), null), "127.0.0.1"));
        // The side that connects asks the same of the node.
        Tls server = tls(node, ours.trustStore(), null);
        assertEquals(CERTIFIED, talk(server, ClientCertificate.NEEDED, tls(client, ours.trustStore(), "OU=node"),
                "127.0.0.1"));
        assertEquals(FAILED, talk(server, ClientCertificate.NEEDED, tls(client, ours.trustStore(), "OU=client"),
                "127.0.0.1"));
    }

    @Test
    void changedFilesAreUsedForTheNextConnectionAndBrokenOnesAreNot() throws Exception {
        Path trust = directory.resolve("changing-trust.p12");
        Files.copy(ours.trustStore(), trust);
        Tls server = tls(node, trust, null);
        Tls one = tls(client, ours.trustStore(), null);
        Tls other = tls(stranger, ours.trustStore(), null);
        assertEquals(CERTIFIED, talk(server, ClientCertificate.NEEDED, one, "127.0.0.1"));
        assertEquals(FAILED, talk(server, ClientCertificate.NEEDED, other, "127.0.0.1"));
        assertEquals(0, server.reloadCount());

        Files.copy(theirs.trustStore(), trust, StandardCopyOption.REPLACE_EXISTING);
        assertEquals(FAILED, talk(server, ClientCertificate.NEEDED, one, "127.0.0.1"));
        assertEquals(CERTIFIED, talk(server, ClientCertificate.NEEDED, other, "127.0.0.1"));
        assertEquals(1, server.reloadCount());
        assertEquals(0, server.reloadFailureCount());

        // What cannot be used leaves the last good files in force.
        Files.write(trust, new byte[] {1, 2, 3});
        assertEquals(CERTIFIED, talk(server, ClientCertificate.NEEDED, other, "127.0.0.1"));
        assertEquals(1, server.reloadCount());
        assertTrue(server.reloadFailureCount() >= 1);
        Files.delete(trust);
        assertEquals(CERTIFIED, talk(server, ClientCertificate.NEEDED, other, "127.0.0.1"));

        Files.copy(ours.trustStore(), trust);
        assertEquals(CERTIFIED, talk(server, ClientCertificate.NEEDED, one, "127.0.0.1"));
        assertEquals(2, server.reloadCount());
    }

    @Test
    void filesThatCannotBeUsedAreRefusedAtTheStart() throws Exception {
        Path garbage = directory.resolve("garbage.p12");
        Files.write(garbage, new byte[] {1, 2, 3});
        Path wrongPassword = directory.resolve("wrong-password");
        Files.writeString(wrongPassword, "not the password\n");
        assertThrows(IOException.class, () -> tls(garbage, ours.trustStore(), null));
        assertThrows(IOException.class, () -> tls(node, garbage, null));
        assertThrows(IOException.class, () -> tls(node, directory.resolve("missing.p12"), null));
        assertThrows(IOException.class,
                () -> Tls.open(new Tls.Settings(node, ours.trustStore(), wrongPassword, null)));
        // The store of an authority holds its key and nothing to trust.
        assertThrows(IOException.class, () -> tls(node, ours.store(), null));
        // A trust store holds no key to show.
        assertThrows(IOException.class, () -> tls(ours.trustStore(), ours.trustStore(), null));
        assertThrows(IllegalArgumentException.class,
                () -> new Tls.Settings(node, ours.trustStore(), null, "no attribute"));
        assertThrows(IllegalArgumentException.class, () -> new Tls.Settings(null, null, null, null));
    }

    @Test
    void aSideWithoutTheStoreForItCanNeitherConnectNorAccept() throws Exception {
        try (Socket socket = new Socket()) {
            assertThrows(IOException.class, () -> tls(node, null, null).connect(socket, "127.0.0.1", 1));
            assertThrows(IOException.class,
                    () -> tls(null, ours.trustStore(), null).accept(socket, ClientCertificate.NONE));
        }
    }
}
