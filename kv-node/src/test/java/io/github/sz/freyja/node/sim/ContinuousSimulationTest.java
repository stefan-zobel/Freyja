package io.github.sz.freyja.node.sim;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The continuous simulation (docs/spec/verification.md, section 3.2): works through fresh
 * seeds of the whole-system simulation for as long as it is told, files every failing seed
 * into the corpus, and writes down what it did.
 *
 * <pre>
 * ./mvnw -pl kv-node -am test -Pcontinuous-simulation -Dfreyja.sim.minutes=60
 * </pre>
 *
 * <ul>
 *   <li>{@code freyja.sim.minutes}: how long to run; default 1.</li>
 *   <li>{@code freyja.sim.start}: the first seed; default: derived from the time of day, so
 *       that every run covers new ground. The report names it.</li>
 *   <li>{@code freyja.sim.report}: the file the report is appended to; default
 *       {@code target/continuous-simulation.txt}.</li>
 * </ul>
 *
 * <p>A failing seed does not end the run: it is added to the corpus of its set, so that it
 * runs on every build from then on, and the run fails at its end.
 */
@Tag("continuous")
class ContinuousSimulationTest {

    private static final Map<String, SystemSimulation.Options> SETS = new LinkedHashMap<>();

    static {
        SETS.put("system-faulty", SystemSimulation.Options.faulty());
        SETS.put("system-hostile", SystemSimulation.Options.hostile());
        SETS.put("system-changing", SystemSimulation.Options.changing());
    }

    @Test
    void workThroughSeedsForTheGivenTime() throws IOException {
        long minutes = Long.getLong("freyja.sim.minutes", 1);
        long start = Long.getLong("freyja.sim.start", System.currentTimeMillis() / 1000 * 1000);
        Path report = Path.of(System.getProperty("freyja.sim.report", "target/continuous-simulation.txt"));
        long end = System.nanoTime() + TimeUnit.MINUTES.toNanos(minutes);

        Map<String, SystemSimulation.Stats> totals = new LinkedHashMap<>();
        Map<String, Long> simulated = new LinkedHashMap<>();
        Map<String, Integer> runs = new LinkedHashMap<>();
        List<String> failures = new ArrayList<>();
        long seed = start;
        while (System.nanoTime() < end) {
            for (Map.Entry<String, SystemSimulation.Options> set : SETS.entrySet()) {
                try {
                    SystemSimulation.Stats stats = new SystemSimulation(seed, set.getValue()).run();
                    totals.computeIfAbsent(set.getKey(), _ -> new SystemSimulation.Stats()).add(stats);
                    simulated.merge(set.getKey(), set.getValue().duration(), Long::sum);
                    runs.merge(set.getKey(), 1, Integer::sum);
                } catch (AssertionError failure) {
                    failures.add(set.getKey() + " seed " + seed + ": " + firstLine(failure.getMessage()));
                    file(set.getKey(), seed);
                }
            }
            seed++;
        }

        List<String> lines = new ArrayList<>();
        lines.add("commit " + commit() + ", seeds " + start + ".." + (seed - 1) + ", " + minutes + " minutes");
        for (String name : SETS.keySet()) {
            // A tick of a node is 10 units and stands for 100 ms.
            long hours = simulated.getOrDefault(name, 0L) * 10 / 1000 / 3600;
            lines.add("  " + name + ": " + runs.getOrDefault(name, 0) + " runs, about " + hours
                    + " simulated hours; " + totals.getOrDefault(name, new SystemSimulation.Stats()));
        }
        for (String failure : failures) {
            lines.add("  FAILED " + failure);
        }
        if (report.getParent() != null) {
            Files.createDirectories(report.getParent());
        }
        Files.write(report, lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        lines.forEach(System.out::println);

        assertTrue(failures.isEmpty(), failures.size() + " failing seeds, now in the corpus: " + failures);
        // Thresholds: a run of some length must reach what the sets are there for.
        SystemSimulation.Stats changing = totals.get("system-changing");
        if (changing != null && runs.get("system-changing") >= 12) {
            assertTrue(changing.replacements > 0 && changing.identitiesLost > 0 && changing.repairs > 0
                    && changing.recoveringLeft > 0 && changing.snapshotsInstalled > 0
                    && changing.sessionsLostInRetry > 0 && changing.slowDiskStepDowns > 0
                    && changing.rejectedForSpace > 0 && changing.transfersAborted > 0
                    && changing.crashesInAnOperation > 0, "a probe was never reached: " + changing);
        }
    }

    private static String firstLine(String message) {
        if (message == null) {
            return "";
        }
        int cut = message.indexOf(" [steps");
        return cut > 0 ? message.substring(0, cut) : message.lines().findFirst().orElse("");
    }

    /** Adds a failing seed to the corpus of its set, if the sources are where the build runs. */
    private static void file(String set, long seed) throws IOException {
        Path corpus = Path.of("src", "test", "resources", "seeds", set + ".txt");
        if (Files.exists(corpus)) {
            Files.writeString(corpus, seed + "\n", StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        }
    }

    /** The commit that is checked out, as far as the files of git say it without running git. */
    private static String commit() {
        try {
            Path git = Path.of("").toAbsolutePath();
            while (git != null && !Files.exists(git.resolve(".git"))) {
                git = git.getParent();
            }
            if (git == null) {
                return "unknown";
            }
            String head = Files.readString(git.resolve(".git").resolve("HEAD")).trim();
            if (head.startsWith("ref: ")) {
                Path ref = git.resolve(".git").resolve(head.substring(5));
                return Files.exists(ref) ? Files.readString(ref).trim() : head.substring(5);
            }
            return head;
        } catch (IOException e) {
            return "unknown";
        }
    }
}
