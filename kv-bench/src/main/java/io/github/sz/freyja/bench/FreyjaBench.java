package io.github.sz.freyja.bench;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Runs a workload against a cluster and reports throughput and latencies
 * (docs/spec/verification.md, section 7).
 *
 * <pre>
 * FreyjaBench --endpoints HOST:PORT[,HOST:PORT]... --workload NAME [--rate N] [--seconds N]
 *             [--clients N] [--keys N] [--value-bytes N] [--seed N] [--timeout-ms N]
 *             [--record FILE] [--baseline FILE]
 *
 * workloads: bulk-load, read-heavy, mixed, write-only, scan-heavy, large-values
 * </pre>
 *
 * <p>{@code --record} stores the result. {@code --baseline} compares the result with a
 * stored one. Exit code 0: done, and not worse than the baseline. 1: worse than the baseline
 * by more than ten percent, or the cluster could not be reached. 2: wrong usage.
 */
public final class FreyjaBench {

    private FreyjaBench() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    public static int run(String[] args, PrintStream out, PrintStream err) {
        Map<String, String> options = new HashMap<>();
        LoadGenerator.Config config;
        try {
            for (int i = 0; i < args.length; i += 2) {
                if (!args[i].startsWith("--") || i + 1 >= args.length) {
                    throw new IllegalArgumentException("options come as --name value");
                }
                options.put(args[i].substring(2), args[i + 1]);
            }
            if (!options.containsKey("endpoints") || !options.containsKey("workload")) {
                throw new IllegalArgumentException("--endpoints and --workload are needed");
            }
            config = new LoadGenerator.Config(List.of(options.get("endpoints").split(",")),
                    LoadGenerator.Workload.valueOf(options.get("workload").toUpperCase(Locale.ROOT).replace('-', '_')),
                    Integer.parseInt(options.getOrDefault("rate", "1000")),
                    Integer.parseInt(options.getOrDefault("seconds", "30")),
                    Integer.parseInt(options.getOrDefault("clients", "32")),
                    Integer.parseInt(options.getOrDefault("keys", "100000")),
                    Integer.parseInt(options.getOrDefault("value-bytes", "128")),
                    Long.parseLong(options.getOrDefault("seed", "1")),
                    Integer.parseInt(options.getOrDefault("timeout-ms", "10000")));
        } catch (IllegalArgumentException e) {
            err.println("error: " + e.getMessage());
            err.println("usage: FreyjaBench --endpoints HOST:PORT[,...] --workload bulk-load|read-heavy|mixed|"
                    + "write-only|scan-heavy|large-values [--rate N] [--seconds N] [--clients N] [--keys N]"
                    + " [--value-bytes N] [--seed N] [--timeout-ms N] [--record FILE] [--baseline FILE]");
            return 2;
        }
        try {
            LoadGenerator.Result result = LoadGenerator.run(config);
            Map<String, String> values = Baseline.describe(result);
            values.forEach((name, value) -> out.println(name + " = " + value));
            if (options.containsKey("record")) {
                Baseline.write(Path.of(options.get("record")), values);
            }
            if (result.completed() == 0) {
                err.println("error: no request was answered");
                return 1;
            }
            if (options.containsKey("baseline")) {
                List<String> worse = Baseline.regressions(Baseline.read(Path.of(options.get("baseline"))), values);
                worse.forEach(line -> err.println("worse than the baseline: " + line));
                return worse.isEmpty() ? 0 : 1;
            }
            return 0;
        } catch (IOException | IllegalArgumentException e) {
            err.println("error: " + e.getMessage());
            return 1;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return 1;
        }
    }
}
