package io.github.sz.freyja.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The stored result of a benchmark run and the comparison of a new run against it
 * (docs/spec/verification.md, section 7): a change that worsens a stored baseline by more
 * than ten percent fails the job.
 *
 * <p>A baseline is a text file of {@code name = value} lines. It says what was run and on
 * what hardware, because a number means nothing without that; a comparison across different
 * settings is refused.
 */
public final class Baseline {

    /** By how much a run may be worse than its baseline. */
    public static final double TOLERANCE = 0.10;

    private Baseline() {
    }

    /** What a run measured, with what it ran and where, as it is stored. */
    public static Map<String, String> describe(LoadGenerator.Result result) {
        LoadGenerator.Config config = result.config();
        Map<String, String> values = new LinkedHashMap<>();
        values.put("workload", config.workload().name());
        values.put("rate.per.second", Integer.toString(config.ratePerSecond()));
        values.put("seconds", Integer.toString(config.seconds()));
        values.put("clients", Integer.toString(config.clients()));
        values.put("keys", Integer.toString(config.keys()));
        values.put("value.bytes", Integer.toString(config.valueBytes()));
        values.put("nodes", Integer.toString(config.endpoints().size()));
        values.put("hardware.os", System.getProperty("os.name") + " " + System.getProperty("os.arch"));
        values.put("hardware.processors", Integer.toString(Runtime.getRuntime().availableProcessors()));
        values.put("hardware.java", System.getProperty("java.version"));
        values.put("due", Long.toString(result.due()));
        values.put("completed", Long.toString(result.completed()));
        values.put("failed", Long.toString(result.failed()));
        values.put("rejected", Long.toString(result.rejected()));
        values.put("shed", Long.toString(result.shed()));
        values.put("warmup.seconds", String.format(java.util.Locale.ROOT, "%.1f", result.warmUpSeconds()));
        values.put("warmup.failures", Long.toString(result.warmUpFailures()));
        if (config.warmUpSeconds() > 0) {
            values.put("warmup.load.seconds", Integer.toString(config.warmUpSeconds()));
        }
        values.put("throughput.per.second", String.format(java.util.Locale.ROOT, "%.1f", result.throughput()));
        values.put("latency.p50.micros", Long.toString(result.latencies().percentile(0.50)));
        values.put("latency.p99.micros", Long.toString(result.latencies().percentile(0.99)));
        values.put("latency.p999.micros", Long.toString(result.latencies().percentile(0.999)));
        values.put("latency.max.micros", Long.toString(result.latencies().max()));
        values.put("service.p99.micros", Long.toString(result.service().percentile(0.99)));
        values.put("service.p999.micros", Long.toString(result.service().percentile(0.999)));
        if (result.rejected() > 0) {
            values.put("rejection.p99.micros", Long.toString(result.rejections().percentile(0.99)));
        }
        return values;
    }

    public static void write(Path file, Map<String, String> values) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("# Freyja benchmark result. Compare only runs with the same settings on the same hardware.");
        values.forEach((name, value) -> lines.add(name + " = " + value));
        Files.write(file, lines, StandardCharsets.UTF_8);
    }

    public static Map<String, String> read(Path file) throws IOException {
        Map<String, String> values = new LinkedHashMap<>();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            int equals = line.indexOf('=');
            if (!line.startsWith("#") && equals > 0) {
                values.put(line.substring(0, equals).trim(), line.substring(equals + 1).trim());
            }
        }
        return values;
    }

    /**
     * Compares a run with a baseline.
     *
     * @return what is worse than the baseline allows, one line each; empty if nothing is
     * @throws IllegalArgumentException if the two were not run with the same settings on the
     *                                  same kind of hardware
     */
    public static List<String> regressions(Map<String, String> baseline, Map<String, String> run) {
        for (String same : List.of("workload", "rate.per.second", "seconds", "clients", "keys", "value.bytes",
                "nodes", "hardware.os", "hardware.processors", "during", "warmup.load.seconds")) {
            if (!String.valueOf(baseline.get(same)).equals(String.valueOf(run.get(same)))) {
                throw new IllegalArgumentException("the baseline was run with " + same + " = " + baseline.get(same)
                        + ", this run with " + run.get(same) + "; they cannot be compared");
            }
        }
        List<String> worse = new ArrayList<>();
        double before = Double.parseDouble(baseline.get("throughput.per.second"));
        double now = Double.parseDouble(run.get("throughput.per.second"));
        if (now < before * (1 - TOLERANCE)) {
            worse.add("throughput fell from " + before + " to " + now + " per second");
        }
        for (String latency : List.of("latency.p50.micros", "latency.p99.micros", "latency.p999.micros")) {
            long was = Long.parseLong(baseline.get(latency));
            long is = Long.parseLong(run.get(latency));
            if (is > was * (1 + TOLERANCE)) {
                worse.add(latency + " rose from " + was + " to " + is);
            }
        }
        long failedBefore = count(baseline, "failed") + count(baseline, "rejected") + count(baseline, "shed");
        long failedNow = count(run, "failed") + count(run, "rejected") + count(run, "shed");
        if (failedNow > failedBefore) {
            worse.add("requests that failed, were rejected, or were shed rose from " + failedBefore + " to "
                    + failedNow);
        }
        return worse;
    }

    /** A count of a result; 0 if the result is older than the count. */
    private static long count(Map<String, String> values, String name) {
        String value = values.get(name);
        return value == null ? 0 : Long.parseLong(value);
    }
}
