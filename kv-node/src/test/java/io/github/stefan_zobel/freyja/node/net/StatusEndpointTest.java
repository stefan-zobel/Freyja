package io.github.stefan_zobel.freyja.node.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.github.stefan_zobel.freyja.client.FreyjaClient;
import io.github.stefan_zobel.freyja.common.Clock;
import io.github.stefan_zobel.freyja.lsm.EngineOptions;
import io.github.stefan_zobel.freyja.lsm.fs.MemoryFileSystem;
import io.github.stefan_zobel.freyja.node.Node;
import io.github.stefan_zobel.freyja.node.NodeServer;
import io.github.stefan_zobel.freyja.node.metrics.MetricsRegistry;
import io.github.stefan_zobel.freyja.node.sm.StateMachineStore;
import io.github.stefan_zobel.freyja.node.snapshot.SnapshotTransfers;
import io.github.stefan_zobel.freyja.node.store.NodeIdentity;
import io.github.stefan_zobel.freyja.raft.ClusterConfig;
import io.github.stefan_zobel.freyja.raft.RaftConfig;

/**
 * Metrics and health over HTTP (docs/spec/operations.md, section 2), with three complete
 * servers in this process.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class StatusEndpointTest {

    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
    private static final Path DATA = Path.of("data");

    private final List<NodeServer> servers = new ArrayList<>();
    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void stop() throws IOException {
        for (NodeServer server : servers) {
            server.close();
        }
        http.close();
    }

    private void startCluster() throws IOException {
        for (int id = 0; id < 3; id++) {
            MemoryFileSystem fs = new MemoryFileSystem();
            Node.initialize(fs, DATA, new NodeIdentity(0x57A7, id), ClusterConfig.ofVoters(0, 1, 2), 1);
            Node.Options node = new Node.Options(RaftConfig.defaults(0, 3), 1 << 20, 4096, 8 << 20, 1_000,
                    SnapshotTransfers.Options.defaults(), new Node.Limits(10_000, 1L << 40, 0, 10, Long.MAX_VALUE),
                    Clock.SYSTEM);
            servers.add(NodeServer.start(fs, new NodeServer.Settings(DATA, new InetSocketAddress(LOOPBACK, 0),
                    new InetSocketAddress(LOOPBACK, 0), new InetSocketAddress(LOOPBACK, 0), Map.of(), node,
                    new StateMachineStore.Options(new EngineOptions().deterministic(true).reserveSize(0)
                            .minFreeSpace(0).memTableSize(16 * 1024).blockCacheCapacity(1 << 20), 60_000), 20,
                    new InetSocketAddress(LOOPBACK, 0), Security.INSECURE)));
        }
        for (NodeServer server : servers) {
            for (NodeServer other : servers) {
                if (other != server) {
                    server.transport().setPeer(other.identity().nodeId(),
                            new InetSocketAddress(LOOPBACK, other.peerPort()));
                }
            }
        }
    }

    private HttpResponse<String> get(NodeServer server, String path) {
        try {
            return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.statusPort() + path))
                    .GET().build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException | InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    private static long value(String metrics, String name) {
        for (String line : metrics.split("\n")) {
            if (line.startsWith(name + " ")) {
                return Long.parseLong(line.substring(name.length() + 1));
            }
        }
        throw new AssertionError("no metric " + name + " in\n" + metrics);
    }

    @Test
    void aNodeReportsItsHealthItsReadinessAndItsMetrics() throws Exception {
        startCluster();
        for (NodeServer server : servers) {
            assertEquals(200, get(server, "/health").statusCode());
            TcpCluster.await("node " + server.identity().nodeId() + " is ready",
                    () -> get(server, "/ready").statusCode() == 200);
        }
        String[] endpoints = servers.stream().map(s -> "127.0.0.1:" + s.clientPort()).toArray(String[]::new);
        try (FreyjaClient client = FreyjaClient.open(FreyjaClient.Options.of(endpoints).withKeepAlive(0))) {
            for (int i = 0; i < 200; i++) {
                client.put(("key-" + i).getBytes(StandardCharsets.UTF_8), new byte[200]);
            }
            for (int i = 0; i < 20; i++) {
                client.get(("key-" + i).getBytes(StandardCharsets.UTF_8));
            }
        }
        NodeServer[] leading = {null};
        TcpCluster.await("the leader publishes what it did", () -> {
            for (NodeServer server : servers) {
                String metrics = get(server, "/metrics").body();
                if (metrics.contains("freyja_raft_role 3\n") && metrics.contains("freyja_client_command_count ")
                        && value(metrics, "freyja_raft_applied_index") >= 200) {
                    leading[0] = server;
                    return true;
                }
            }
            return false;
        });
        String metrics = get(leading[0], "/metrics").body();
        assertTrue(value(metrics, "freyja_client_command_count") >= 200, metrics);
        assertTrue(value(metrics, "freyja_client_read_count") >= 20);
        assertTrue(value(metrics, "freyja_client_command_p50_micros")
                <= value(metrics, "freyja_client_command_p99_micros"));
        assertTrue(value(metrics, "freyja_raft_term") >= 1);
        assertEquals(leading[0].identity().nodeId(), value(metrics, "freyja_raft_leader"));
        assertTrue(value(metrics, "freyja_raft_elections_won_total") >= 1);
        assertTrue(value(metrics, "freyja_write_groups_total") > 0, "the storage engine reports here too");
        assertTrue(value(metrics, "freyja_transport_sent") > 0);
        assertEquals(0, value(metrics, "freyja_alarm_nospace"));
        assertEquals(0, value(metrics, "freyja_raft_log_recovering"));
        int other = (leading[0].identity().nodeId() + 1) % 3;
        assertTrue(value(metrics, "freyja_raft_follower_" + other + "_lag") >= 0, "the lag of every follower");

        // Only GET is answered.
        HttpResponse<String> post = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"
                + leading[0].statusPort() + "/metrics")).POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(405, post.statusCode());

        // Without the others the node runs, but it cannot serve.
        NodeServer alone = leading[0];
        for (NodeServer server : servers) {
            if (server != alone) {
                server.close();
            }
        }
        TcpCluster.await("the node that is left alone is not ready", () -> get(alone, "/ready").statusCode() == 503);
        assertTrue(get(alone, "/ready").body().contains("no leader"), get(alone, "/ready").body());
        assertEquals(200, get(alone, "/health").statusCode());
        assertNull(alone.runtime().notAlive());
    }

    @Test
    void theRegistryRendersCountersGaugesAndDurations() {
        MetricsRegistry registry = new MetricsRegistry();
        registry.count("write.bytes", 10);
        registry.count("write.bytes", 5);
        registry.gauge("level.0.files", 3);
        registry.gauge("level.0.files", 4);
        for (int i = 1; i <= 1000; i++) {
            registry.duration("wal.sync", i * 1_000L); // 1 to 1000 microseconds
        }
        assertEquals(15, registry.counter("write.bytes"));
        assertEquals(4L, registry.gaugeValue("level.0.files"));
        assertNull(registry.gaugeValue("never"));
        assertEquals(1000, registry.durationCount("wal.sync"));
        String text = registry.render();
        assertEquals(15, value(text, "freyja_write_bytes_total"));
        assertEquals(4, value(text, "freyja_level_0_files"));
        assertEquals(1000, value(text, "freyja_wal_sync_count"));
        long p50 = value(text, "freyja_wal_sync_p50_micros");
        long p99 = value(text, "freyja_wal_sync_p99_micros");
        assertTrue(p50 >= 500 && p50 <= 510, "p50 " + p50);
        assertTrue(p99 >= 990 && p99 <= 1000, "p99 " + p99);
        assertEquals(1000, value(text, "freyja_wal_sync_max_micros"));
        List<String> names = text.lines().map(line -> line.substring(0, line.indexOf(' '))).toList();
        assertEquals(names.stream().sorted().toList(), names, "sorted by name");
    }
}
