package io.github.sz.freyja.raft.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.github.sz.freyja.common.testing.Seeds;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.sim.TraceValidator.Verdict;

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

    private Verdict validate(String name, TlaTraceWriter trace) throws IOException, InterruptedException {
        assertTrue(Files.exists(TraceValidator.tlcJar()),
                "TLC not found at " + TraceValidator.tlcJar() + "; set -Dfreyja.tlc=<tla2tools.jar>");
        return TraceValidator.validate(temp.resolve(name), trace);
    }

    private static TlaTraceWriter record(SimOptions options, long millis) {
        Simulation sim = new Simulation(options);
        Set<Integer> voters = new TreeSet<>();
        for (int i = 0; i < options.nodes(); i++) {
            voters.add(i);
        }
        TlaTraceWriter writer = new TlaTraceWriter(options.nodes() + options.spareNodes(),
                new ClusterConfig(voters, Set.of()));
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

    @Test
    void tracesOfRunsWithMembershipChangesTransfersAndLogDamageAreBehaviorsOfTheModel() {
        int[] validated = new int[1];
        Seeds.forEach(TraceValidationTest.class, "trace-validation-membership", 10, seed -> {
            // The model lets a damaged log keep a true prefix; it does not know logs that go back.
            TlaTraceWriter trace = record(SimOptions.reconfiguring(seed).withLogReverts(false), 2_500);
            List<TlaTraceWriter.Step> steps = trace.steps();
            if (steps.size() > MAX_STEPS) {
                steps.subList(MAX_STEPS, steps.size()).clear();
            }
            Verdict verdict = validate("membership-seed-" + seed, trace);
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
