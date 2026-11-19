package io.github.sz.freyja.bench;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
 *             [--warmup-seconds N] [--record FILE] [--baseline FILE]
 *             [--status HOST:PORT[,...]]
 *             [--during compaction|snapshot-transfer|catch-up [--admin HOST:PORT[,...]]
 *              [--spare ID,PEER-HOST:PORT,CLIENT-HOST:PORT] [--background-rate N]]
 *             [--overload search|N [--target-p99-ms N] [--probe-seconds N]
 *              [--reject-p99-ms N] [--admitted-p999-ms N]]
 *
 * workloads: bulk-load, read-heavy, mixed, write-only, scan-heavy, large-values
 * </pre>
 *
 * <ul>
 *   <li>{@code --warmup-seconds} runs the same load for that long before the measured part
 *       begins, so that the result shows a cluster that is warm.</li>
 *   <li>{@code --record} stores the result. {@code --baseline} compares the result with a
 *       stored one.</li>
 *   <li>{@code --status} names the HTTP endpoints of the nodes. With them the result has the
 *       write and space amplification ({@link Amplification}).</li>
 *   <li>{@code --during} measures the workload while the cluster compacts, sends a snapshot
 *       to a node that joins, or brings a node that joins up to date ({@link Background}).
 *       {@code --admin} names the administration listeners of the nodes, {@code --spare} the
 *       empty node that joins.</li>
 *   <li>{@code --overload} is the overload test ({@link Overload}): it searches the
 *       sustainable rate, or takes the one given, and runs at twice that.</li>
 * </ul>
 *
 * <p>Exit code 0: done, and not worse than the baseline. 1: worse than the baseline by more
 * than ten percent, the condition of {@code --during} did not come about, the overload test
 * failed, or the cluster could not be reached. 2: wrong usage.
 */
public final class FreyjaBench {

    private FreyjaBench() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    private static List<String> list(Map<String, String> options, String name) {
        String value = options.get(name);
        return value == null || value.isBlank() ? List.of() : List.of(value.split(","));
    }

    private static int number(Map<String, String> options, String name, int otherwise) {
        return Integer.parseInt(options.getOrDefault(name, Integer.toString(otherwise)));
    }

    public static int run(String[] args, PrintStream out, PrintStream err) {
        Map<String, String> options = new HashMap<>();
        LoadGenerator.Config config;
        Background background = null;
        Overload.Bounds bounds = null;
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
                    number(options, "rate", 1000), number(options, "seconds", 30), number(options, "clients", 32),
                    number(options, "keys", 100_000), number(options, "value-bytes", 128),
                    Long.parseLong(options.getOrDefault("seed", "1")), number(options, "timeout-ms", 10_000))
                    .withWarmUp(number(options, "warmup-seconds", 0));
            if (options.containsKey("during")) {
                if (options.containsKey("overload")) {
                    throw new IllegalArgumentException("--during and --overload do not go together");
                }
                background = new Background(
                        Background.During.valueOf(options.get("during").toUpperCase(Locale.ROOT).replace('-', '_')),
                        config.endpoints(), list(options, "admin"), list(options, "status"),
                        options.containsKey("spare") ? Background.Spare.parse(options.get("spare")) : null,
                        number(options, "background-rate", 1000), config.keys());
            }
            if (options.containsKey("overload")) {
                String overload = options.get("overload");
                if (!overload.equals("search") && Integer.parseInt(overload) < 1) {
                    throw new IllegalArgumentException("--overload takes search or a rate");
                }
                bounds = new Overload.Bounds(1_000L * number(options, "target-p99-ms", 100),
                        number(options, "probe-seconds", 5), 1_000L * number(options, "reject-p99-ms", 100),
                        1_000L * number(options, "admitted-p999-ms", 1_000));
            }
        } catch (IllegalArgumentException e) {
            err.println("error: " + e.getMessage());
            err.println("usage: FreyjaBench --endpoints HOST:PORT[,...] --workload bulk-load|read-heavy|mixed|"
                    + "write-only|scan-heavy|large-values [--rate N] [--seconds N] [--clients N] [--keys N]"
                    + " [--value-bytes N] [--seed N] [--timeout-ms N] [--warmup-seconds N] [--record FILE]"
                    + " [--baseline FILE]"
                    + " [--status HOST:PORT[,...]]"
                    + " [--during compaction|snapshot-transfer|catch-up [--admin HOST:PORT[,...]]"
                    + " [--spare ID,PEER-HOST:PORT,CLIENT-HOST:PORT] [--background-rate N]]"
                    + " [--overload search|N [--target-p99-ms N] [--probe-seconds N] [--reject-p99-ms N]"
                    + " [--admitted-p999-ms N]]");
            return 2;
        }
        try {
            if (bounds != null) {
                return overload(config, options, bounds, out, err);
            }
            List<String> statuses = list(options, "status");
            Amplification amplification = statuses.isEmpty() ? null : Amplification.before(statuses);
            Thread starter = null;
            if (background != null) {
                Background condition = background;
                long delayMillis = config.warmUpSeconds() * 1_000L + config.seconds() * 1_000L / 4;
                starter = Thread.ofVirtual().name("freyja-bench-condition").start(() -> {
                    try {
                        Thread.sleep(delayMillis);
                        condition.start();
                    } catch (IOException e) {
                        condition.failed(e.getMessage());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            LoadGenerator.Result result = LoadGenerator.run(config);
            Map<String, String> values = Baseline.describe(result);
            boolean reached = true;
            if (background != null) {
                starter.join();
                Map<String, String> report = background.finish();
                values.putAll(report);
                reached = Boolean.parseBoolean(report.get("during.reached"));
            }
            if (amplification != null) {
                values.putAll(amplification.after(result.writtenBytes()
                        + (background == null ? 0 : background.writtenBytes()), config.endpoints()));
            }
            values.forEach((name, value) -> out.println(name + " = " + value));
            if (options.containsKey("record")) {
                Baseline.write(Path.of(options.get("record")), values);
            }
            if (result.completed() == 0) {
                err.println("error: no request was answered");
                return 1;
            }
            if (!reached) {
                err.println("error: the condition " + values.get("during") + " did not come about"
                        + (values.containsKey("during.trouble") ? ": " + values.get("during.trouble") : "")
                        + "; the figures are those of a run without it");
                return 1;
            }
            if (options.containsKey("baseline")) {
                List<String> worse = Baseline.regressions(Baseline.read(Path.of(options.get("baseline"))), values);
                worse.forEach(line -> err.println("worse than the baseline: " + line));
                return worse.isEmpty() ? 0 : 1;
            }
            return 0;
        } catch (IOException | IllegalArgumentException | IllegalStateException e) {
            err.println("error: " + e.getMessage());
            return 1;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return 1;
        }
    }

    private static int overload(LoadGenerator.Config config, Map<String, String> options, Overload.Bounds bounds,
                                PrintStream out, PrintStream err) throws IOException, InterruptedException {
        String given = options.get("overload");
        int sustainable = given.equals("search") ? Overload.sustainableRate(config, bounds, err)
                : Integer.parseInt(given);
        LoadGenerator.Result result = Overload.run(config, sustainable);
        Map<String, String> values = new LinkedHashMap<>(Baseline.describe(result));
        values.put("overload.sustainable.per.second", Integer.toString(sustainable));
        List<String> problems = Overload.problems(result, bounds);
        values.put("overload.passed", Boolean.toString(problems.isEmpty()));
        values.forEach((name, value) -> out.println(name + " = " + value));
        if (options.containsKey("record")) {
            Baseline.write(Path.of(options.get("record")), values);
        }
        problems.forEach(problem -> err.println("overload: " + problem));
        return problems.isEmpty() ? 0 : 1;
    }
}
