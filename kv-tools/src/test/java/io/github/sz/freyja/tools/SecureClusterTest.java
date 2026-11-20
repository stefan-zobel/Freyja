package io.github.sz.freyja.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import javax.net.ssl.SSLSocket;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.github.sz.freyja.client.FreyjaClient;
import io.github.sz.freyja.client.FreyjaException;
import io.github.sz.freyja.common.tls.TestCertificates;
import io.github.sz.freyja.common.tls.Tls;
import io.github.sz.freyja.common.wire.Frames;
import io.github.sz.freyja.common.wire.Handshake;
import io.github.sz.freyja.node.FreyjaNode;

/**
 * A cluster of real processes with TLS on every listener (docs/spec/operations.md,
 * section 5): one authority for all certificates, and a name in each certificate that says
 * whether it is that of a node, of a client, or of an administrator.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class SecureClusterTest {

    private static final String CLUSTER = "5ec";
    private static final String FIRST_TOKEN = "the-first-token-of-the-test";
    private static final String SECOND_TOKEN = "the-second-token-of-the-test";

    /** One node process. */
    private final class Node {
        final int id;
        final Path data;
        Process process;
        int peer;
        int client;
        int admin;
        int status;

        Node(int id, Path data) {
            this.id = id;
            this.data = data;
        }

        void start(int peerPort, List<String> seeds) throws IOException {
            List<String> command = new ArrayList<>(List.of(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(), "--enable-preview",
                    "-Xmx256m", "-cp", System.getProperty("java.class.path"), FreyjaNode.class.getName(), "run",
                    "--data", data.toString(), "--peer", "127.0.0.1:" + peerPort, "--client", "127.0.0.1:0",
                    "--admin", "127.0.0.1:0", "--status", "127.0.0.1:0", "--tick-ms", "20",
                    "--tls-key", nodeKey.toString(), "--tls-password-file", certificates.passwordFile().toString(),
                    "--peer-trust", trust.toString(), "--peer-name", "OU=node",
                    "--client-trust", trust.toString(), "--client-name", "OU=client",
                    "--client-tokens", tokens.toString(),
                    "--admin-trust", trust.toString(), "--admin-name", "OU=admin"));
            for (String seed : seeds) {
                command.add("--seed");
                command.add(seed);
            }
            process = new ProcessBuilder(command).redirectErrorStream(true).start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(),
                    StandardCharsets.UTF_8));
            StringBuilder said = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                said.append(line).append('\n');
                if (line.startsWith("READY ")) {
                    for (String part : line.split(" ")) {
                        if (part.startsWith("peer=")) {
                            peer = Integer.parseInt(part.substring(5));
                        } else if (part.startsWith("client=")) {
                            client = Integer.parseInt(part.substring(7));
                        } else if (part.startsWith("admin=")) {
                            admin = Integer.parseInt(part.substring(6));
                        } else if (part.startsWith("status=")) {
                            status = Integer.parseInt(part.substring(7));
                        }
                    }
                    // Whatever the process prints from now on must not block it.
                    Thread.ofVirtual().start(() -> {
                        try {
                            while (reader.readLine() != null) {
                                // discard
                            }
                        } catch (IOException e) {
                            // the process is gone
                        }
                    });
                    return;
                }
            }
            throw new AssertionError("node " + id + " ended without saying that it listens:\n" + said);
        }

        long metric(String name) throws IOException {
            try (InputStream in = URI.create("http://127.0.0.1:" + status + "/metrics").toURL().openStream()) {
                for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
                    if (line.startsWith(name + " ")) {
                        return Long.parseLong(line.substring(name.length() + 1).trim());
                    }
                }
            }
            throw new AssertionError("no metric " + name);
        }
    }

    @TempDir
    Path directory;

    private final List<Node> nodes = new ArrayList<>();
    private TestCertificates certificates;
    private Path trust;
    private Path nodeKey;
    private Path clientKey;
    private Path adminKey;
    private Path tokens;

    @AfterEach
    void killAll() throws InterruptedException {
        for (Node node : nodes) {
            if (node.process != null) {
                node.process.destroyForcibly();
                node.process.waitFor();
            }
        }
    }

    private static int program(String... args) {
        PrintStream quiet = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
        return FreyjaNode.run(args, quiet, quiet, false);
    }

    /** Runs the tool of the administrator: its exit code, then its output. */
    private static String tool(String... args) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8);
        int code = FreyjaAdmin.run(args, out, out);
        return code + "\n" + bytes.toString(StandardCharsets.UTF_8);
    }

    /** Runs the tool of a user: its exit code, then its output. */
    private static String cli(String... args) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8);
        int code = FreyjaCli.run(args, out, out);
        return (code + "\n" + bytes.toString(StandardCharsets.UTF_8)).replace("\r\n", "\n").strip();
    }

    /** Runs the tool as an administrator with a certificate. */
    private String admin(int port, String... command) {
        List<String> args = new ArrayList<>(List.of("--tls-key", adminKey.toString(), "--tls-trust",
                trust.toString(), "--tls-password-file", certificates.passwordFile().toString(),
                "127.0.0.1:" + port));
        args.addAll(List.of(command));
        return tool(args.toArray(String[]::new));
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("not within 60 seconds: " + what);
            }
            Thread.sleep(50);
        }
    }

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private Node leader() throws InterruptedException {
        Node[] found = {null};
        await("a leader", () -> {
            for (Node node : nodes) {
                if (admin(node.admin, "status").contains("role:            LEADER")) {
                    found[0] = node;
                    return true;
                }
            }
            return false;
        });
        return found[0];
    }

    private String[] endpoints() {
        return nodes.stream().map(n -> "127.0.0.1:" + n.client).toArray(String[]::new);
    }

    /** A client that proves itself with the certificate in {@code key} and with {@code token}; either may be null. */
    private FreyjaClient client(Path key, String token, int timeoutMillis) {
        return FreyjaClient.open(FreyjaClient.Options.of(endpoints()).withRequestTimeout(timeoutMillis)
                .withKeepAlive(0)
                .withTls(new Tls.Settings(key, trust, certificates.passwordFile(), "OU=node"), token));
    }

    /** Whether a client with this proof is served. */
    private boolean served(Path key, String token) {
        try (FreyjaClient client = client(key, token, 1_500)) {
            client.get(b("key"));
            return true;
        } catch (FreyjaException e) {
            return false;
        }
    }

    @Test
    void aNodeWithoutTlsMustBeToldSo() {
        Path data = directory.resolve("unused");
        assertEquals(2, program("run", "--data", data.toString(), "--peer", "127.0.0.1:0", "--client",
                "127.0.0.1:0", "--admin", "127.0.0.1:0"));
        assertEquals(2, program("run", "--data", data.toString(), "--peer", "127.0.0.1:0", "--client",
                "127.0.0.1:0", "--admin", "127.0.0.1:0", "--insecure", "no"));
        assertEquals(2, program("run", "--data", data.toString(), "--peer", "127.0.0.1:0", "--client",
                "127.0.0.1:0", "--admin", "127.0.0.1:0", "--insecure", "yes", "--tls-key", "any"));
        // With TLS, the node says whom it accepts at every listener.
        assertEquals(2, program("run", "--data", data.toString(), "--peer", "127.0.0.1:0", "--client",
                "127.0.0.1:0", "--admin", "127.0.0.1:0", "--tls-key", "any", "--peer-trust", "any",
                "--admin-trust", "any"));
        assertEquals(2, program("run", "--data", data.toString(), "--peer", "127.0.0.1:0", "--client",
                "127.0.0.1:0", "--admin", "127.0.0.1:0", "--tls-key", "any", "--client-tokens", "any"));
    }

    @Test
    void everyListenerTakesOnlyWhoProvesToBeWhatItIsFor() throws Exception {
        certificates = new TestCertificates(directory.resolve("certificates"));
        TestCertificates.Authority authority = certificates.authority("cluster");
        trust = authority.trustStore();
        nodeKey = certificates.issue(authority, "node", "CN=a node,OU=node,O=Test", "ip:127.0.0.1");
        clientKey = certificates.issue(authority, "client", "CN=an application,OU=client,O=Test", null);
        adminKey = certificates.issue(authority, "admin", "CN=an administrator,OU=admin,O=Test", null);
        tokens = directory.resolve("tokens");
        Files.writeString(tokens, "# the tokens of the test\n\n" + FIRST_TOKEN + "\n");

        int[] ports = new int[3];
        try (ServerSocket a = new ServerSocket(0); ServerSocket c = new ServerSocket(0);
             ServerSocket d = new ServerSocket(0)) {
            ports[0] = a.getLocalPort();
            ports[1] = c.getLocalPort();
            ports[2] = d.getLocalPort();
        }
        List<String> seeds = List.of("0=127.0.0.1:" + ports[0], "1=127.0.0.1:" + ports[1], "2=127.0.0.1:" + ports[2]);
        for (int id = 0; id < 3; id++) {
            Path data = directory.resolve("node-" + id);
            assertEquals(0, program("init", "--data", data.toString(), "--cluster", CLUSTER, "--node",
                    Integer.toString(id), "--voters", "0,1,2"));
            nodes.add(new Node(id, data));
        }
        for (Node node : nodes) {
            node.start(ports[node.id], seeds);
        }
        // A leader: the nodes have taken each other's certificates.
        Node leader = leader();
        for (Node node : nodes) {
            assertTrue(admin(leader.admin, "set-address", Integer.toString(node.id), "127.0.0.1:" + node.peer,
                    "127.0.0.1:" + node.client).startsWith("0\n"));
        }
        assertEquals(0, leader.metric("freyja_security_insecure"));

        // Clients: a certificate of a client, or a token.
        try (FreyjaClient certified = client(clientKey, null, 30_000);
             FreyjaClient withToken = client(null, FIRST_TOKEN, 30_000)) {
            certified.put(b("key"), b("written with a certificate"));
            assertEquals("written with a certificate",
                    new String(withToken.get(b("key")).orElseThrow().value(), StandardCharsets.UTF_8));
            withToken.put(b("key"), b("written with a token"));
            assertEquals("written with a token",
                    new String(certified.get(b("key")).orElseThrow().value(), StandardCharsets.UTF_8));
        }
        // The tool of a user, with a certificate and with a token.
        Path tokenFile = directory.resolve("token-of-a-user");
        Files.writeString(tokenFile, FIRST_TOKEN + "\n");
        String stores = certificates.passwordFile().toString();
        String cluster = String.join(",", endpoints());
        assertEquals("0\nok", cli("--tls-trust", trust.toString(), "--tls-key", clientKey.toString(),
                "--tls-password-file", stores, "--tls-name", "OU=node", cluster, "put", "cli", "with a certificate"));
        assertEquals("0\nwith a certificate", cli("--tls-trust", trust.toString(), "--tls-password-file", stores,
                "--token-file", tokenFile.toString(), cluster, "get", "cli"));
        assertTrue(cli("--tls-trust", trust.toString(), "--tls-password-file", stores, "--timeout-ms", "1500",
                cluster, "get", "cli").startsWith("1\nerror: "), "neither a certificate nor a token");
        assertTrue(served(clientKey, null));
        assertTrue(served(null, FIRST_TOKEN));
        assertTrue(!served(null, null), "neither a certificate nor a token");
        assertTrue(!served(null, "a-token-nobody-has-issued"), "an unknown token");
        assertTrue(!served(adminKey, null), "the certificate of an administrator is not that of a client");
        assertTrue(!served(nodeKey, null), "the certificate of a node is not that of a client");
        assertTrue(!served(adminKey, FIRST_TOKEN), "a certificate that is shown must be that of a client,"
                + " whatever the token");
        // Without TLS nobody is served.
        try (FreyjaClient plain = FreyjaClient.open(FreyjaClient.Options.of(endpoints()).withRequestTimeout(1_500)
                .withKeepAlive(0))) {
            assertThrows(FreyjaException.class, () -> plain.get(b("key")));
        }
        // A client that expects another name in the certificate of a node does not talk to it.
        try (FreyjaClient picky = FreyjaClient.open(FreyjaClient.Options.of(endpoints()).withRequestTimeout(1_500)
                .withKeepAlive(0).withTls(new Tls.Settings(clientKey, trust, certificates.passwordFile(),
                        "OU=another"), null))) {
            assertThrows(FreyjaException.class, () -> picky.get(b("key")));
        }

        // Administrators: a certificate of an administrator, nothing else.
        String password = certificates.passwordFile().toString();
        String address = "127.0.0.1:" + leader.admin;
        assertTrue(admin(leader.admin, "status").startsWith("0\n"));
        assertTrue(tool("--tls-key", adminKey.toString(), "--tls-trust", trust.toString(), "--tls-password-file",
                password, "--tls-name", "OU=node", address, "status").startsWith("0\n"));
        assertTrue(tool("--tls-key", adminKey.toString(), "--tls-trust", trust.toString(), "--tls-password-file",
                password, "--tls-name", "OU=another", address, "status").startsWith("1\n"));
        assertTrue(tool("--tls-key", clientKey.toString(), "--tls-trust", trust.toString(), "--tls-password-file",
                password, address, "status").startsWith("1\n"), "the certificate of a client");
        assertTrue(tool("--tls-key", nodeKey.toString(), "--tls-trust", trust.toString(), "--tls-password-file",
                password, address, "status").startsWith("1\n"), "the certificate of a node");
        String plain = tool(address, "status");
        assertTrue(plain.startsWith("1\n"), "no TLS");
        assertTrue(plain.contains("--tls-key and --tls-trust are needed"), plain);
        assertTrue(tool("--tls-key", adminKey.toString(), address, "status").startsWith("2\n"));
        assertTrue(tool("--tls-key", directory.resolve("missing").toString(), "--tls-trust", trust.toString(),
                address, "status").startsWith("1\n"));

        // Peers: a certificate of a node. A client cannot pose as one.
        long refusedBefore = leader.metric("freyja_transport_refused");
        Tls asClient = Tls.open(new Tls.Settings(clientKey, trust, certificates.passwordFile(), null));
        try (Socket raw = new Socket()) {
            raw.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), leader.peer), 5_000);
            raw.setSoTimeout(10_000);
            byte[] answer;
            try {
                SSLSocket secured = asClient.connect(raw, "127.0.0.1", leader.peer);
                int other = (leader.id + 1) % 3;
                Frames.write(secured.getOutputStream(), new Handshake(Handshake.PEER,
                        Long.parseUnsignedLong(CLUSTER, 16), other, "test").encode());
                secured.getOutputStream().flush();
                answer = Frames.read(secured.getInputStream(), Handshake.MAX_BYTES);
            } catch (IOException e) {
                answer = null;
            }
            assertNull(answer, "the node answered a peer with the certificate of a client");
        }
        await("the refusal is counted", () -> {
            try {
                return leader.metric("freyja_transport_refused") > refusedBefore;
            } catch (IOException e) {
                return false;
            }
        });

        // The token file changes: the new token is taken and the old one is not, without a restart.
        Files.writeString(tokens, SECOND_TOKEN + "\n");
        await("the new token is taken", () -> served(null, SECOND_TOKEN));
        await("the old token is not taken any more", () -> !served(null, FIRST_TOKEN));

        // The certificate of the nodes changes: new connections are made with the new one.
        Path renewed = certificates.issue(authority, "node-renewed", "CN=a node renewed,OU=node,O=Test",
                "ip:127.0.0.1");
        long[] reloadsBefore = new long[nodes.size()];
        for (Node node : nodes) {
            reloadsBefore[node.id] = node.metric("freyja_security_reloads");
        }
        Files.copy(renewed, nodeKey, StandardCopyOption.REPLACE_EXISTING);
        await("every node has read its new certificate", () -> {
            try {
                for (Node node : nodes) {
                    // A new connection is what makes a node look at its files.
                    admin(node.admin, "status");
                    if (node.metric("freyja_security_reloads") <= reloadsBefore[node.id]) {
                        return false;
                    }
                }
                return true;
            } catch (IOException e) {
                return false;
            }
        });
        try (FreyjaClient certified = client(clientKey, null, 30_000)) {
            certified.put(b("after"), b("renewal"));
            assertEquals("written with a token",
                    new String(certified.get(b("key")).orElseThrow().value(), StandardCharsets.UTF_8));
        }
        assertTrue(admin(leader().admin, "status").startsWith("0\n"));
    }
}
