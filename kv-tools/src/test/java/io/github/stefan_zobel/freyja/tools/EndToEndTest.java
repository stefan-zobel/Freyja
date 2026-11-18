package io.github.stefan_zobel.freyja.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.github.stefan_zobel.freyja.client.FreyjaClient;
import io.github.stefan_zobel.freyja.client.FreyjaException;
import io.github.stefan_zobel.freyja.node.FreyjaNode;

/**
 * A cluster of real processes on real files (docs/spec/roadmap.md, milestone C4): the node
 * program, the client library, and the tool of the administrator together.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class EndToEndTest {

    private static final String CLUSTER = "e2e";

    /** One node process. */
    private final class Process0 {
        final int id;
        final Path data;
        Process process;
        int peer;
        int client;
        int admin;

        Process0(int id, Path data) {
            this.id = id;
            this.data = data;
        }

        /**
         * Starts the process and waits for the line that says where it listens.
         *
         * @param peerPort the port for peers; 0 for any free port
         */
        void start(int peerPort, List<String> seeds) throws IOException {
            List<String> command = new ArrayList<>(List.of(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(), "--enable-preview",
                    "-Xmx256m", "-cp", System.getProperty("java.class.path"), FreyjaNode.class.getName(), "run",
                    "--data", data.toString(), "--peer", "127.0.0.1:" + peerPort, "--client", "127.0.0.1:0",
                    "--admin", "127.0.0.1:0", "--tick-ms", "20"));
            for (String seed : seeds) {
                command.add("--seed");
                command.add(seed);
            }
            process = new ProcessBuilder(command).redirectErrorStream(true).start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(),
                    StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("READY ")) {
                    for (String part : line.split(" ")) {
                        if (part.startsWith("peer=")) {
                            peer = Integer.parseInt(part.substring(5));
                        } else if (part.startsWith("client=")) {
                            client = Integer.parseInt(part.substring(7));
                        } else if (part.startsWith("admin=")) {
                            admin = Integer.parseInt(part.substring(6));
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
            throw new AssertionError("node " + id + " ended without saying that it listens");
        }

        void kill() throws InterruptedException {
            process.destroyForcibly();
            process.waitFor();
        }
    }

    @TempDir
    Path directory;

    private final List<Process0> nodes = new ArrayList<>();

    @AfterEach
    void killAll() throws InterruptedException {
        for (Process0 node : nodes) {
            if (node.process != null) {
                node.kill();
            }
        }
    }

    /** Prepares a data directory with the node program, in this process. */
    private static int init(String... args) {
        PrintStream quiet = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
        return FreyjaNode.run(args, quiet, quiet, false);
    }

    private static String admin(int port, String... command) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8);
        String[] args = new String[command.length + 1];
        args[0] = "127.0.0.1:" + port;
        System.arraycopy(command, 0, args, 1, command.length);
        int code = FreyjaAdmin.run(args, out, out);
        return code + "\n" + bytes.toString(StandardCharsets.UTF_8);
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

    private Process0 leader() throws InterruptedException {
        Process0[] found = {null};
        await("a leader", () -> {
            for (Process0 node : nodes) {
                if (node.process != null && node.process.isAlive()
                        && admin(node.admin, "status").contains("role:            LEADER")) {
                    found[0] = node;
                    return true;
                }
            }
            return false;
        });
        return found[0];
    }

    @Test
    void aClusterOfProcessesServesClientsLosesANodeAndGrows() throws Exception {
        // The ports for peers are chosen first: every node must know where the others listen
        // before the first leader can be elected.
        int[] ports = new int[3];
        try (java.net.ServerSocket a = new java.net.ServerSocket(0); java.net.ServerSocket c = new java.net.ServerSocket(0);
             java.net.ServerSocket d = new java.net.ServerSocket(0)) {
            ports[0] = a.getLocalPort();
            ports[1] = c.getLocalPort();
            ports[2] = d.getLocalPort();
        }
        List<String> seeds = List.of("0=127.0.0.1:" + ports[0], "1=127.0.0.1:" + ports[1], "2=127.0.0.1:" + ports[2]);
        for (int id = 0; id < 3; id++) {
            Path data = directory.resolve("node-" + id);
            assertEquals(0, init("init", "--data", data.toString(), "--cluster", CLUSTER, "--node",
                    Integer.toString(id), "--voters", "0,1,2"));
            nodes.add(new Process0(id, data));
        }
        for (Process0 node : nodes) {
            node.start(ports[node.id], seeds);
        }
        Process0 first = leader();

        // The cluster learns the addresses for good, through the log.
        for (Process0 node : nodes) {
            assertTrue(admin(first.admin, "set-address", Integer.toString(node.id), "127.0.0.1:" + node.peer,
                    "127.0.0.1:" + node.client).startsWith("0\n"));
        }
        // Taken by the leader is not yet applied; the status shows when it is.
        await("the addresses are in the replicated state", () -> admin(first.admin, "status")
                .contains("member:          node 2 voter, peers at 127.0.0.1:" + nodes.get(2).peer));

        String[] endpoints = nodes.stream().map(n -> "127.0.0.1:" + n.client).toArray(String[]::new);
        try (FreyjaClient client = FreyjaClient.open(FreyjaClient.Options.of(endpoints).withRequestTimeout(60_000)
                .withKeepAlive(0))) {
            for (int i = 0; i < 50; i++) {
                client.put(b("key-" + i), b("value-" + i));
            }
            assertEquals("value-7", new String(client.get(b("key-7")).orElseThrow().value(), StandardCharsets.UTF_8));

            // The leader is killed, without any warning. The client goes on.
            first.kill();
            for (int i = 50; i < 100; i++) {
                client.put(b("key-" + i), b("value-" + i));
            }
            assertEquals(100, client.scan(b("key-"), b("key-~"), 1_000).size());

            // It comes back from its files and catches up.
            first.start(ports[first.id], seeds);
            await("the node that was killed has caught up", () -> {
                try (FreyjaClient direct = FreyjaClient.open(FreyjaClient.Options.of("127.0.0.1:" + first.client)
                        .withRequestTimeout(1_000).withKeepAlive(0))) {
                    return direct.getStale(b("key-99")).isPresent();
                } catch (FreyjaException e) {
                    return false;
                }
            });

            // A fourth node joins: prepared with no voters, announced, added, promoted.
            Path data = directory.resolve("node-3");
            assertEquals(0, init("init", "--data", data.toString(), "--cluster", CLUSTER, "--node", "3"));
            Process0 joiner = new Process0(3, data);
            nodes.add(joiner);
            joiner.start(0, seeds);
            Process0 now = leader();
            String added = admin(now.admin, "add-learner", "3", "127.0.0.1:" + joiner.peer,
                    "127.0.0.1:" + joiner.client);
            assertTrue(added.startsWith("0\n"), added);
            await("the learner is promoted", () -> admin(leaderOrFirst().admin, "promote", "3").startsWith("0\n")
                    || admin(leaderOrFirst().admin, "status").contains("node 3 voter"));
            await("the new voter serves a stale read", () -> {
                try (FreyjaClient direct = FreyjaClient.open(FreyjaClient.Options.of("127.0.0.1:" + joiner.client)
                        .withRequestTimeout(1_000).withKeepAlive(0))) {
                    return direct.getStale(b("key-42")).isPresent();
                } catch (FreyjaException e) {
                    return false;
                }
            });
            assertTrue(admin(joiner.admin, "status").contains("node 3 voter"), admin(joiner.admin, "status"));
            client.put(b("after"), b("growth"));

            // The tool says what a node refuses.
            Process0 follower = nodes.stream().filter(n -> n != leaderOrFirst()).findFirst().orElseThrow();
            String refused = admin(follower.admin, "clear-alarm");
            assertTrue(refused.startsWith("1\n") && refused.contains("not the leader"), refused);
            assertTrue(admin(follower.admin, "scrub").contains("every block is intact"));
            assertTrue(admin(follower.admin, "snapshot").startsWith("0\n"));
            assertTrue(admin(follower.client, "status").startsWith("1\n"), "the listener for clients is not for it");
            assertTrue(admin(follower.admin, "no-such-command").startsWith("2\n"));
        }
    }

    private Process0 leaderOrFirst() {
        try {
            return leader();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }
}
