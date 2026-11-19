package io.github.sz.freyja.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The overload test of the benchmark (docs/spec/verification.md, section 7): at twice the
 * sustainable load the cluster rejects requests quickly and keeps the latency of the ones it
 * admits bounded.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class OverloadTest {

    private BenchCluster cluster;

    @AfterEach
    void stop() throws Exception {
        if (cluster != null) {
            cluster.close();
        }
    }

    private static Histogram histogram(long... values) {
        Histogram histogram = new Histogram();
        for (long value : values) {
            histogram.record(value);
        }
        return histogram;
    }

    private static LoadGenerator.Result result(long failed, long shed, Histogram service, Histogram rejections) {
        LoadGenerator.Config config = new LoadGenerator.Config(List.of("127.0.0.1:1"),
                LoadGenerator.Workload.MIXED, 100, 1, 1, 10, 10, 1, 1_000);
        return new LoadGenerator.Result(config, 100, service.count(), failed, rejections.count(), shed, 1,
                service, service, rejections, 0, 0, 0);
    }

    @Test
    void theVerdictNamesEveryProblem() {
        Overload.Bounds bounds = new Overload.Bounds(100_000, 1, 50_000, 500_000);
        assertEquals(List.of(), Overload.problems(result(0, 0, histogram(1_000, 400_000), histogram(2_000)), bounds));
        List<String> all = Overload.problems(result(3, 7, histogram(1_000, 900_000), histogram(80_000)), bounds);
        assertEquals(4, all.size(), all.toString());
        assertTrue(all.get(0).startsWith("3 requests were neither answered nor rejected"), all.toString());
        assertTrue(all.get(1).startsWith("rejections are not quick"), all.toString());
        assertTrue(all.get(2).startsWith("admitted requests took too long"), all.toString());
        assertTrue(all.get(3).contains("too few clients"), all.toString());
    }

    @Test
    void atTwiceTheSustainableLoadTheClusterRejectsQuicklyAndAdmitsWithBoundedLatency() throws Exception {
        // The nodes admit few requests at once, so that a few hundred clients overload them.
        cluster = BenchCluster.start(new BenchCluster.Settings(64, 1 << 20, true));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8);
        int exit = FreyjaBench.run(new String[] {"--endpoints", String.join(",", cluster.clients()),
                "--workload", "write-only", "--rate", "200", "--seconds", "3", "--clients", "256", "--keys", "1000",
                "--value-bytes", "100", "--overload", "search", "--probe-seconds", "2", "--target-p99-ms", "200",
                "--reject-p99-ms", "500", "--admitted-p999-ms", "3000"}, out, out);
        String output = bytes.toString(StandardCharsets.UTF_8);
        System.out.println(output);
        assertEquals(0, exit, output);
        java.util.Map<String, String> values = new java.util.HashMap<>();
        for (String line : output.split("\n")) {
            int equals = line.indexOf(" = ");
            if (equals > 0) {
                values.put(line.substring(0, equals).trim(), line.substring(equals + 3).trim());
            }
        }
        int sustainable = Integer.parseInt(values.get("overload.sustainable.per.second"));
        assertTrue(sustainable >= 1, output);
        assertEquals(2 * sustainable, Integer.parseInt(values.get("rate.per.second")), output);
        assertEquals("true", values.get("overload.passed"), output);
        assertEquals("0", values.get("failed"), output);
        assertTrue(output.contains("probe: 200 per second"), "the search starts at the given rate: " + output);
    }
}
