package io.github.stefan_zobel.freyja.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.github.stefan_zobel.freyja.common.Clock;
import io.github.stefan_zobel.freyja.lsm.EngineOptions;
import io.github.stefan_zobel.freyja.lsm.fs.MemoryFileSystem;
import io.github.stefan_zobel.freyja.node.Node;
import io.github.stefan_zobel.freyja.node.NodeServer;
import io.github.stefan_zobel.freyja.node.sm.StateMachineStore;
import io.github.stefan_zobel.freyja.node.snapshot.SnapshotTransfers;
import io.github.stefan_zobel.freyja.node.store.NodeIdentity;
import io.github.stefan_zobel.freyja.raft.ClusterConfig;
import io.github.stefan_zobel.freyja.raft.RaftConfig;

/**
 * The load generator, its histogram, and the comparison with a baseline
 * (docs/spec/verification.md, section 7), against three nodes in this process.
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class LoadGeneratorTest {

    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
    private static final Path DATA = Path.of("data");

    private final List<NodeServer> servers = new ArrayList<>();

    @TempDir
    Path directory;

    private List<String> startCluster() throws IOException {
        for (int id = 0; id < 3; id++) {
            MemoryFileSystem fs = new MemoryFileSystem();
            Node.initialize(fs, DATA, new NodeIdentity(0xBE, id), ClusterConfig.ofVoters(0, 1, 2), 1);
            Node.Options node = new Node.Options(RaftConfig.defaults(0, 3), 1 << 20, 4096, 8 << 20, 1_000,
                    SnapshotTransfers.Options.defaults(), new Node.Limits(10_000, 1L << 40, 0, 10, Long.MAX_VALUE),
                    Clock.SYSTEM);
            servers.add(NodeServer.start(fs, new NodeServer.Settings(DATA, new InetSocketAddress(LOOPBACK, 0),
                    new InetSocketAddress(LOOPBACK, 0), new InetSocketAddress(LOOPBACK, 0), Map.of(), node,
                    new StateMachineStore.Options(new EngineOptions().deterministic(true).reserveSize(0)
                            .minFreeSpace(0).blockCacheCapacity(8 << 20), 60_000), 20)));
        }
        List<String> endpoints = new ArrayList<>();
        for (NodeServer server : servers) {
            endpoints.add("127.0.0.1:" + server.clientPort());
            for (NodeServer other : servers) {
                if (other != server) {
                    server.transport().setPeer(other.identity().nodeId(),
                            new InetSocketAddress(LOOPBACK, other.peerPort()));
                }
            }
        }
        return endpoints;
    }

    @AfterEach
    void stop() throws IOException {
        for (NodeServer server : servers) {
            server.close();
        }
    }

    @Test
    void everyWorkloadRunsAndReportsWhatItMeasured() throws Exception {
        List<String> endpoints = startCluster();
        for (LoadGenerator.Workload workload : LoadGenerator.Workload.values()) {
            LoadGenerator.Result result = LoadGenerator.run(new LoadGenerator.Config(endpoints, workload, 300, 1, 8,
                    200, 64, 7, 30_000));
            assertEquals(300, result.due(), workload.name());
            assertEquals(300, result.completed() + result.failed() + result.shed(), workload.name());
            assertEquals(300, result.completed(), workload + ": every request of a quiet cluster is answered");
            assertTrue(result.throughput() > 0);
            Histogram latencies = result.latencies();
            assertTrue(latencies.percentile(0.5) > 0, "a request takes time");
            assertTrue(latencies.percentile(0.5) <= latencies.percentile(0.99)
                    && latencies.percentile(0.99) <= latencies.percentile(0.999)
                    && latencies.percentile(0.999) <= latencies.max(), workload.name());
        }
    }

    @Test
    void theToolRecordsAResultAndComparesWithABaseline() throws Exception {
        List<String> endpoints = startCluster();
        Path baseline = directory.resolve("baseline.txt");
        String[] common = {"--endpoints", String.join(",", endpoints), "--workload", "mixed", "--rate", "200",
                "--seconds", "1", "--clients", "4", "--keys", "100"};
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8);

        String[] record = new String[common.length + 2];
        System.arraycopy(common, 0, record, 0, common.length);
        record[common.length] = "--record";
        record[common.length + 1] = baseline.toString();
        assertEquals(0, FreyjaBench.run(record, out, out), bytes.toString(StandardCharsets.UTF_8));
        Map<String, String> stored = Baseline.read(baseline);
        assertEquals("MIXED", stored.get("workload"));
        assertEquals("200", stored.get("completed"));
        assertTrue(bytes.toString(StandardCharsets.UTF_8).contains("latency.p999.micros = "));

        // A baseline that was much better than this run: the run is worse, and the tool says so.
        Map<String, String> better = new HashMap<>(stored);
        better.put("latency.p99.micros", "1");
        better.put("throughput.per.second", "100000.0");
        Path strict = directory.resolve("strict.txt");
        Baseline.write(strict, better);
        bytes.reset();
        String[] compare = record.clone();
        compare[common.length] = "--baseline";
        compare[common.length + 1] = strict.toString();
        assertEquals(1, FreyjaBench.run(compare, out, out));
        String report = bytes.toString(StandardCharsets.UTF_8);
        assertTrue(report.contains("worse than the baseline: throughput fell"), report);
        assertTrue(report.contains("latency.p99.micros rose from 1"), report);

        // A baseline that was much worse: nothing to complain about.
        Map<String, String> worse = new HashMap<>(stored);
        worse.put("latency.p50.micros", "10000000");
        worse.put("latency.p99.micros", "10000000");
        worse.put("latency.p999.micros", "10000000");
        worse.put("throughput.per.second", "1.0");
        assertEquals(List.of(), Baseline.regressions(worse, stored));
        // Runs with other settings are not compared.
        Map<String, String> other = new HashMap<>(stored);
        other.put("clients", "64");
        assertThrows(IllegalArgumentException.class, () -> Baseline.regressions(other, stored));
        assertEquals(2, FreyjaBench.run(new String[] {"--workload", "mixed"}, out, out));
        assertEquals(2, FreyjaBench.run(new String[] {"--endpoints", "x:1", "--workload", "no-such"}, out, out));
    }

    @Test
    void theHistogramKeepsPercentilesWithinAboutOnePercent() {
        Histogram histogram = new Histogram();
        assertEquals(0, histogram.percentile(0.99));
        long[] values = new long[200_000];
        Random random = new Random(5);
        for (int i = 0; i < values.length; i++) {
            // From microseconds to minutes, more of the small ones.
            values[i] = (long) Math.exp(random.nextDouble() * 18);
            histogram.record(values[i]);
        }
        java.util.Arrays.sort(values);
        assertEquals(values.length, histogram.count());
        assertEquals(values[values.length - 1], histogram.max());
        for (double quantile : new double[] {0.01, 0.5, 0.9, 0.99, 0.999, 1.0}) {
            long exact = values[(int) Math.max(0, Math.ceil(quantile * values.length) - 1)];
            long reported = histogram.percentile(quantile);
            assertTrue(reported >= exact && reported <= exact * 1.02 + 1,
                    "quantile " + quantile + ": exact " + exact + ", reported " + reported);
        }
        Histogram other = new Histogram();
        other.record(5);
        other.record(1L << 40);
        histogram.merge(other);
        assertEquals(values.length + 2, histogram.count());
        assertEquals(1L << 40, histogram.max());
    }
}
