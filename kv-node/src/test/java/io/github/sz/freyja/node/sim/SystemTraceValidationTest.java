package io.github.sz.freyja.node.sim;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.github.sz.freyja.common.testing.Seeds;
import io.github.sz.freyja.raft.sim.TlaTraceWriter;
import io.github.sz.freyja.raft.sim.TraceValidator;

/**
 * Trace validation of whole-system runs against the TLA+ model (docs/spec/verification.md,
 * section 2.7): TLC must accept what the cores of complete nodes did, on real log stores and
 * with their messages sent as bytes.
 *
 * <p>TLC is an external tool. This test runs only in the profile {@code trace-validation};
 * the system property {@code freyja.tlc} names the jar ({@code tools/tla2tools.jar} in the
 * repository directory by default).
 */
@Tag("tlc")
class SystemTraceValidationTest {

    @TempDir
    Path temp;

    /** Longer runs are cut off here; a prefix of a run is a run. */
    private static final int MAX_STEPS = 3_000;

    @Test
    void tracesOfWholeSystemRunsAreBehaviorsOfTheModel() {
        assertTrue(Files.exists(TraceValidator.tlcJar()),
                "TLC not found at " + TraceValidator.tlcJar() + "; set -Dfreyja.tlc=<tla2tools.jar>");
        int[] validated = new int[1];
        Seeds.forEach(SystemTraceValidationTest.class, "system-trace-validation", 6, seed -> {
            TlaTraceWriter trace = SystemTraceTest.record(seed);
            List<TlaTraceWriter.Step> steps = trace.steps();
            if (steps.size() > MAX_STEPS) {
                steps.subList(MAX_STEPS, steps.size()).clear();
            }
            TraceValidator.Verdict verdict = TraceValidator.validate(temp.resolve("seed-" + seed), trace);
            assertTrue(verdict.accepted(), "seed " + seed + ", " + verdict.steps() + " steps: " + verdict.reason());
            validated[0] += verdict.steps();
        });
        assertTrue(validated[0] > 5_000, "steps validated: " + validated[0]);
    }
}
