package io.github.sz.freyja.node.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.sim.TlaTraceWriter;
import io.github.sz.freyja.raft.sim.TraceRecorder;

/**
 * The traces of whole-system runs (docs/spec/verification.md, section 2.7): what is recorded
 * of complete nodes has the form of the traces of the Raft simulator. Whether the model
 * accepts them is the matter of {@link SystemTraceValidationTest}, which needs TLC.
 */
class SystemTraceTest {

    private static String json(long seed, SystemSimulation.Stats[] stats) {
        StringBuilder text = new StringBuilder();
        SystemSimulation simulation = new SystemSimulation(seed, SystemSimulation.Options.traced());
        simulation.setStepListener(new TraceRecorder(text));
        stats[0] = simulation.run();
        return text.toString();
    }

    static TlaTraceWriter record(long seed) {
        SystemSimulation.Options options = SystemSimulation.Options.traced();
        Integer[] ids = new Integer[options.nodes()];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = i;
        }
        TlaTraceWriter writer = new TlaTraceWriter(options.nodes() + options.spares(), ClusterConfig.ofVoters(ids));
        SystemSimulation simulation = new SystemSimulation(seed, options);
        simulation.setStepListener(writer);
        simulation.run();
        return writer;
    }

    private static int count(String text, String part) {
        int count = 0;
        for (int at = text.indexOf(part); at >= 0; at = text.indexOf(part, at + 1)) {
            count++;
        }
        return count;
    }

    @Test
    void recordingDoesNotChangeARun() {
        for (long seed = 1; seed <= 3; seed++) {
            SystemSimulation.Stats[] recorded = new SystemSimulation.Stats[1];
            json(seed, recorded);
            SystemSimulation.Stats plain = new SystemSimulation(seed, SystemSimulation.Options.traced()).run();
            assertEquals(plain.toString(), recorded[0].toString(), "seed " + seed);
        }
    }

    @Test
    void aTraceIsAFunctionOfItsSeed() {
        SystemSimulation.Stats[] stats = new SystemSimulation.Stats[1];
        String first = json(5, stats);
        assertEquals(first, json(5, stats));
        assertNotEquals(first, json(6, stats));
    }

    @Test
    void aTraceHoldsTheStepsOfEveryNodeAndEveryStartAfterACrash() {
        long crashes = 0;
        for (long seed = 1; seed <= 6; seed++) {
            SystemSimulation.Stats[] stats = new SystemSimulation.Stats[1];
            String trace = json(seed, stats);
            String where = "seed " + seed;
            for (int node = 0; node < 3; node++) {
                assertTrue(count(trace, "\"node\":" + node + ",") > 100, where);
            }
            assertTrue(trace.contains("\"type\":\"MessageReceived\""), where);
            assertTrue(trace.contains("\"type\":\"Persisted\""), where);
            assertTrue(trace.contains("\"type\":\"ClientPropose\""), where);
            assertTrue(trace.contains("\"type\":\"ClientReadIndex\""), where);
            assertTrue(trace.contains("\"role\":\"LEADER\""), where);
            // No log is shortened in these runs: the model knows no snapshots.
            assertEquals(0, count(trace, "\"type\":\"Snapshot"), where);
            assertEquals(0, stats[0].snapshotsCreated + stats[0].snapshotsInstalled, where);
            // Every node that went down came back, and each time that is a line.
            assertEquals(stats[0].crashes, count(trace, "\"type\":\"Restart\""), where);
            crashes += stats[0].crashes;
        }
        assertTrue(crashes > 6, "crashes: " + crashes);
    }

    /**
     * What the trace specification demands of the messages (docs/tla/README.md, section 3.1)
     * and can be seen without TLC: what a node receives, a node has released in an earlier
     * step. That holds only if every Ready is recorded with the step that handed it out or
     * with a later step of its node, and never ahead of a step of another node that follows.
     */
    @Test
    void everyReceivedMessageWasReleasedByAnEarlierStep() {
        long received = 0;
        long persists = 0;
        for (long seed = 1; seed <= 6; seed++) {
            TlaTraceWriter trace = record(seed);
            Set<String> released = new HashSet<>();
            int at = 0;
            for (TlaTraceWriter.Step step : trace.steps()) {
                at++;
                if (step.event.equals("Recv")) {
                    assertTrue(released.contains(step.message),
                            "seed " + seed + ", step " + at + " receives what nobody released: " + step.message);
                    received++;
                }
                if (step.event.equals("Persist")) {
                    assertTrue(step.persisted <= 1, "seed " + seed + ", step " + at
                            + ": a node reports one write at a time, here " + step.persisted);
                    persists += step.persisted;
                }
                assertTrue(step.durableIndex <= step.lastLogIndex && step.commitIndex <= step.lastLogIndex,
                        "seed " + seed + ", step " + at + ": " + TlaTraceWriter.record(step));
                released.addAll(step.released);
            }
            assertTrue(trace.module().contains("TraceInitVoters == {0, 1, 2}"));
        }
        assertTrue(received > 1_000, "received: " + received);
        assertTrue(persists > 100, "writes reported durable: " + persists);
    }
}
