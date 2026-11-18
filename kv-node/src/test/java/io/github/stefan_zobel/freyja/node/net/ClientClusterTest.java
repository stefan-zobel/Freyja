package io.github.stefan_zobel.freyja.node.net;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.github.stefan_zobel.freyja.client.FreyjaClient;
import io.github.stefan_zobel.freyja.client.FreyjaException;
import io.github.stefan_zobel.freyja.common.kv.Command;
import io.github.stefan_zobel.freyja.common.kv.Command.Condition;
import io.github.stefan_zobel.freyja.common.kv.Command.Delete;
import io.github.stefan_zobel.freyja.common.kv.Command.Put;

/**
 * The client library against real nodes over sockets (docs/spec/node.md, section 9).
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class ClientClusterTest {

    private TcpCluster cluster;

    @AfterEach
    void stop() throws IOException {
        if (cluster != null) {
            cluster.close();
        }
    }

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String s(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private FreyjaClient client(int keepAliveMillis, int... nodes) {
        String[] endpoints = new String[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            endpoints[i] = cluster.endpoint(nodes[i]);
        }
        return FreyjaClient.open(new FreyjaClient.Options(List.of(endpoints), TcpCluster.CLUSTER, 30_000, 1_000, 10,
                200, 500, keepAliveMillis));
    }

    @Test
    void everyOperationWorksWhicheverNodeTheClientKnows() throws IOException {
        cluster = new TcpCluster(60_000);
        for (int node = 0; node < 3; node++) {
            // The client knows one node only; that node sends it to the leader.
            try (FreyjaClient client = client(0, node)) {
                String prefix = "n" + node + "-";
                client.put(b(prefix + "b"), b("two"));
                client.put(b(prefix + "a"), b("one"));
                client.write(List.of(new Put(b(prefix + "c"), b("three")), new Put(b(prefix + "d"), b("four")),
                        new Delete(b(prefix + "b"))));

                assertEquals("one", s(client.get(b(prefix + "a")).orElseThrow().value()));
                assertTrue(client.get(b(prefix + "b")).isEmpty());
                List<FreyjaClient.Entry> entries = client.scan(b(prefix), b(prefix + "~"), 10);
                assertEquals(List.of(prefix + "a", prefix + "c", prefix + "d"),
                        entries.stream().map(e -> s(e.key())).toList());
                assertEquals(2, client.scan(b(prefix), b(prefix + "~"), 2).size());
                assertEquals(List.of(prefix + "d", prefix + "c"),
                        client.scanReverse(b(prefix), b(prefix + "~"), 2).stream().map(e -> s(e.key())).toList());

                assertFalse(client.putIfAbsent(b(prefix + "a"), b("again")));
                assertTrue(client.putIfAbsent(b(prefix + "e"), b("five")));
                long version = client.get(b(prefix + "a")).orElseThrow().modIndex();
                assertTrue(client.putIfUnchanged(b(prefix + "a"), b("uno"), version));
                assertFalse(client.putIfUnchanged(b(prefix + "a"), b("eins"), version), "it has changed since");
                assertEquals("uno", s(client.get(b(prefix + "a")).orElseThrow().value()));
                assertTrue(client.conditional(
                        List.of(Condition.valueEquals(b(prefix + "a"), b("uno")), Condition.exists(b(prefix + "c"))),
                        List.of(new Delete(b(prefix + "c"))), List.of()));
                assertTrue(client.get(b(prefix + "c")).isEmpty());

                assertThrows(FreyjaException.Refused.class,
                        () -> client.put(new byte[Command.MAX_KEY_SIZE + 1], b("v")), "a key that is too long");
                client.put(b(prefix + "f"), b("after the refusal"));
            }
        }
        // What one client wrote, another reads, also stale from a node that only follows.
        try (FreyjaClient client = client(0, 0, 1, 2)) {
            assertEquals("five", s(client.get(b("n2-e")).orElseThrow().value()));
            TcpCluster.await("a stale read sees the value", () -> client.getStale(b("n0-f")).isPresent());
        }
    }

    @Test
    void aCounterCountsEveryIncrementOnceWhileTheLeaderIsLostAndComesBack() throws IOException {
        cluster = new TcpCluster(60_000);
        try (FreyjaClient client = client(0, 0, 1, 2)) {
            byte[] key = b("counter");
            client.put(key, b("0"));
            int increments = 0;
            int stoppedAt = -1;
            TcpCluster.Server stopped = null;
            for (int i = 0; i < 90; i++) {
                if (i == 30) {
                    // The node the client talks to last is the leader. It goes away.
                    FreyjaClient.Versioned now = client.get(key).orElseThrow();
                    for (TcpCluster.Server server : cluster.servers) {
                        boolean[] leads = {false};
                        try {
                            leads[0] = server.runtime.onNode(node -> node.view().role().name().equals("LEADER"))
                                    .get(5, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                        if (leads[0]) {
                            stopped = server;
                        }
                    }
                    assertTrue(stopped != null, "a leader, at counter " + s(now.value()));
                    stopped.stop();
                    stoppedAt = i;
                }
                if (i == 60) {
                    stopped.restart();
                }
                // Read, add one, write if nobody else has written: with a session, a command
                // that is sent twice counts once.
                FreyjaClient.Versioned current = client.get(key).orElseThrow();
                int value = Integer.parseInt(s(current.value()));
                assertEquals(increments, value);
                assertTrue(client.putIfUnchanged(key, b(Integer.toString(value + 1)), current.modIndex()));
                increments++;
            }
            assertEquals(30, stoppedAt);
            assertEquals("90", s(client.get(key).orElseThrow().value()));
            int id = stopped.id;
            TcpCluster.await("the node that came back has caught up",
                    () -> staleValue(id, key).equals("90"));
        }
    }

    private String staleValue(int node, byte[] key) {
        try (FreyjaClient client = FreyjaClient.open(new FreyjaClient.Options(List.of(cluster.endpoint(node)),
                0, 2_000, 500, 10, 100, 500, 0))) {
            return client.getStale(key).map(v -> s(v.value())).orElse("");
        } catch (FreyjaException e) {
            return "";
        }
    }

    @Test
    void aSessionIsKeptAliveOrReplacedWhenItHasExpired() throws IOException, InterruptedException {
        cluster = new TcpCluster(600);
        try (FreyjaClient kept = client(100, 0, 1, 2); FreyjaClient idle = client(0, 0, 1, 2)) {
            kept.put(b("kept"), b("1"));
            idle.put(b("idle"), b("1"));
            long keptSession = kept.sessionId();
            long idleSession = idle.sessionId();
            assertTrue(keptSession != idleSession);

            // Both are idle for longer than a session lasts. One says that it is alive, and
            // that also moves the time of the state machine on.
            Thread.sleep(1_500);
            kept.put(b("kept"), b("2"));
            assertEquals(keptSession, kept.sessionId(), "the session was kept alive");

            idle.put(b("idle"), b("2"));
            assertTrue(idle.sessionId() != idleSession, "the session had expired; the client opened a new one");
            assertArrayEquals(b("2"), idle.get(b("idle")).orElseThrow().value());
        }
    }
}
