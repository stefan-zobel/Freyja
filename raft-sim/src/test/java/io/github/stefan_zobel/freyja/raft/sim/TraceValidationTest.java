package io.github.stefan_zobel.freyja.raft.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.github.stefan_zobel.freyja.common.testing.Seeds;

/**
 * Trace validation against the TLA+ model (docs/spec/verification.md, section 2.7): TLC must
 * accept the traces of simulated runs and must reject mutated ones.
 *
 * <p>TLC is an external tool. These tests run only in the profile {@code trace-validation};
 * the system property {@code freyja.tlc} names the jar ({@code tools/tla2tools.jar} in the
 * repository directory by default).
 */
@Tag("tlc")
class TraceValidationTest {

    @TempDir
    Path temp;

    /** Longer runs are cut off here. */
    private static final int MAX_STEPS = 3_000;

    private static Path repositoryRoot() {
        Path here = Path.of("").toAbsolutePath();
        return Files.exists(here.resolve("docs")) ? here : here.getParent();
    }

    /** The outcome of one validation. */
    private record Verdict(boolean accepted, String reason, int steps) {
    }

    private Verdict validate(String name, TlaTraceWriter trace) throws IOException, InterruptedException {
        Path root = repositoryRoot();
        Path tla = root.resolve("docs").resolve("tla");
        Path jar = Path.of(System.getProperty("freyja.tlc",
                root.resolve("tools").resolve("tla2tools.jar").toString()));
        assertTrue(Files.exists(jar), "TLC not found at " + jar + "; set -Dfreyja.tlc=<tla2tools.jar>");
        Path dir = Files.createDirectories(temp.resolve(name));
        for (String file : List.of("Raft.tla", "RaftTrace.tla", "RaftTrace.cfg")) {
            Files.copy(tla.resolve(file), dir.resolve(file));
        }
        Files.writeString(dir.resolve("TraceData.tla"), trace.module(), StandardCharsets.UTF_8);
        Path output = dir.resolve("tlc.out");
        Process process = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-XX:+UseParallelGC", "-Xss64m", "-cp", jar.toString(), "tlc2.TLC",
                "-workers", "1", "-metadir", dir.resolve("states").toString(),
                "-config", "RaftTrace.cfg", "RaftTrace.tla")
                .directory(dir.toFile())
                .redirectErrorStream(true)
                .redirectOutput(output.toFile())
                .start();
        if (!process.waitFor(20, TimeUnit.MINUTES)) {
            process.destroyForcibly().waitFor();
            throw new AssertionError("TLC did not finish within 20 minutes");
        }
        String text = Files.readString(output, StandardCharsets.UTF_8);
        int steps = trace.steps().size();
        if (text.contains("Invariant NotDone is violated")) {
            return new Verdict(true, "accepted", steps);
        }
        String reason = "TLC did not get past the last step";
        for (String line : text.split("\n")) {
            if (line.contains("is violated") || line.contains("rror:") || line.contains("depth of the complete")) {
                reason = line.trim();
                if (!line.contains("depth of the complete")) {
                    break;
                }
            }
        }
        return new Verdict(false, reason + rejectedStep(trace, text), steps);
    }

    /** The step TLC could not take, with the steps of the same node before it. */
    private static String rejectedStep(TlaTraceWriter trace, String tlcOutput) {
        java.util.regex.Matcher depth = java.util.regex.Pattern
                .compile("depth of the complete state graph search is (\\d+)").matcher(tlcOutput);
        if (!depth.find()) {
            return "";
        }
        int at = Integer.parseInt(depth.group(1)) - 1;
        List<TlaTraceWriter.Step> steps = trace.steps();
        if (at < 0 || at >= steps.size()) {
            return "";
        }
        StringBuilder text = new StringBuilder("\nrejected step " + (at + 1) + ": "
                + TlaTraceWriter.record(steps.get(at)));
        int shown = 0;
        for (int i = at - 1; i >= 0 && shown < 6; i--) {
            if (steps.get(i).node == steps.get(at).node) {
                text.append("\n  step ").append(i + 1).append(": ").append(TlaTraceWriter.record(steps.get(i)));
                shown++;
            }
        }
        return text.toString();
    }

    private static TlaTraceWriter record(SimOptions options, long millis) {
        Simulation sim = new Simulation(options);
        TlaTraceWriter writer = new TlaTraceWriter(options.nodes());
        sim.setStepListener(writer);
        sim.run(millis);
        return writer;
    }

    @Test
    void tracesOfSimulatedRunsAreBehaviorsOfTheModel() {
        int[] validated = new int[1];
        Seeds.forEach(TraceValidationTest.class, "trace-validation", 12, seed -> {
            TlaTraceWriter trace = record(SimOptions.faulty(seed), 1_500);
            // The time TLC needs grows with the square of the length, because every state
            // holds all messages sent so far. A prefix of a run is a run.
            List<TlaTraceWriter.Step> steps = trace.steps();
            if (steps.size() > MAX_STEPS) {
                steps.subList(MAX_STEPS, steps.size()).clear();
            }
            Verdict verdict = validate("seed-" + seed, trace);
            assertTrue(verdict.accepted(), "seed " + seed + ", " + verdict.steps() + " steps: " + verdict.reason());
            validated[0] += verdict.steps();
        });
        assertTrue(validated[0] > 5_000, "steps validated: " + validated[0]);
    }

    /** Records a run that has an election and committed entries, changes it, and validates it. */
    private Verdict mutated(String name, Consumer<List<TlaTraceWriter.Step>> mutation) throws Exception {
        TlaTraceWriter trace = record(SimOptions.calm(3, 77), 600);
        mutation.accept(trace.steps());
        return validate(name, trace);
    }

    private static TlaTraceWriter.Step first(List<TlaTraceWriter.Step> steps,
                                             java.util.function.Predicate<TlaTraceWriter.Step> wanted) {
        for (TlaTraceWriter.Step step : steps) {
            if (wanted.test(step)) {
                return step;
            }
        }
        throw new AssertionError("the run has no such step");
    }

    @Test
    void theUnchangedRunIsAccepted() throws Exception {
        Verdict verdict = mutated("unchanged", steps -> {
            assertNotNull(first(steps, s -> s.role.equals("L")));
            assertNotNull(first(steps, s -> s.commitIndex >= 3));
        });
        assertTrue(verdict.accepted(), verdict.reason());
    }

    @Test
    void aSecondLeaderInATermIsRejected() throws Exception {
        Verdict verdict = mutated("second-leader", steps -> {
            TlaTraceWriter.Step leader = first(steps, s -> s.role.equals("L"));
            // Another node claims the same term from its next step on.
            int other = (leader.node + 1) % 3;
            boolean after = false;
            for (TlaTraceWriter.Step step : steps) {
                after |= step == leader;
                if (after && step.node == other && step.term == leader.term) {
                    step.role = "L";
                    step.votedFor = other;
                }
            }
        });
        assertFalse(verdict.accepted());
    }

    @Test
    void anAcknowledgmentBeforePersistenceIsRejected() throws Exception {
        Verdict verdict = mutated("early-acknowledgment", steps -> {
            // The step that made the entries durable released the acknowledgment. Move the
            // acknowledgment to the step that received the entries.
            for (int i = 1; i < steps.size(); i++) {
                TlaTraceWriter.Step persist = steps.get(i);
                String ack = persist.released.stream()
                        .filter(m -> m.contains("\"AER\"") && m.contains("ok |-> TRUE")).findFirst().orElse(null);
                if (!persist.event.equals("Persist") || ack == null) {
                    continue;
                }
                for (int j = i - 1; j >= 0; j--) {
                    TlaTraceWriter.Step receive = steps.get(j);
                    if (receive.node == persist.node && receive.event.equals("Recv")
                            && receive.message.contains("\"AE\"")) {
                        persist.released.remove(ack);
                        receive.released.add(ack);
                        return;
                    }
                }
            }
            throw new AssertionError("the run has no acknowledgment released by a Persisted event");
        });
        assertFalse(verdict.accepted());
    }

    @Test
    void aVoteBeforePersistenceIsRejected() throws Exception {
        Verdict verdict = mutated("early-vote", steps -> {
            for (int i = 1; i < steps.size(); i++) {
                TlaTraceWriter.Step persist = steps.get(i);
                String vote = persist.released.stream()
                        .filter(m -> m.contains("\"RVR\"") && m.contains("pre |-> FALSE")
                                && m.contains("granted |-> TRUE")).findFirst().orElse(null);
                if (!persist.event.equals("Persist") || vote == null) {
                    continue;
                }
                for (int j = i - 1; j >= 0; j--) {
                    TlaTraceWriter.Step receive = steps.get(j);
                    if (receive.node == persist.node && receive.event.equals("Recv")
                            && receive.message.contains("\"RV\"")) {
                        persist.released.remove(vote);
                        receive.released.add(vote);
                        return;
                    }
                }
            }
            throw new AssertionError("the run has no vote released by a Persisted event");
        });
        assertFalse(verdict.accepted());
    }

    @Test
    void aChangedCommittedEntryIsRejected() throws Exception {
        Verdict verdict = mutated("changed-entry", steps -> {
            // A follower claims another term for the entry it has just committed.
            TlaTraceWriter.Step committed = first(steps, s -> s.role.equals("F") && s.commitIndex >= 2
                    && s.commitIndex == s.lastLogIndex);
            committed.lastLogTerm += 1;
        });
        assertFalse(verdict.accepted());
    }

    @Test
    void aCommitWithoutAMajorityIsRejected() throws Exception {
        Verdict verdict = mutated("early-commit", steps -> {
            // The leader claims to have committed its first entry in the step that created it.
            TlaTraceWriter.Step leader = first(steps, s -> s.role.equals("L"));
            assertEquals(0, leader.commitIndex);
            leader.commitIndex = leader.lastLogIndex;
        });
        assertFalse(verdict.accepted());
    }
}
