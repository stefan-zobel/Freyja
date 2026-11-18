package io.github.stefan_zobel.freyja.raft.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.common.testing.Seeds;

/**
 * Clusters on a reliable network with delayed persistence (docs/spec/verification.md,
 * section 2.4). The safety properties are checked by the simulation after every step; the
 * tests add liveness after the network has become calm.
 */
class RaftSimulationTest {

    @Test
    void aCalmClusterElectsOneLeaderAndCommitsEveryProposal() {
        for (int size : new int[] {1, 3, 5, 7}) {
            Simulation sim = new Simulation(SimOptions.calm(size, 42 + size));
            sim.run(5_000);
            sim.setWorkload(false);
            sim.run(1_000);
            sim.checkConverged();
            Simulation.Stats stats = sim.stats();
            assertEquals(1, sim.checks().leaderOfTerm().size(), "one election: " + stats);
            assertTrue(stats.accepted > 300, stats.toString());
            assertEquals(stats.accepted, stats.appliedCommands, stats.toString());
            assertEquals(0, stats.truncations);
            // The entry of the leader's term plus every command.
            assertEquals(stats.accepted + 1, sim.checks().committed().size());
        }
    }

    /** Runs one seed: a rough phase, then a calm one in which the cluster must converge. */
    private static Simulation runSeed(long seed) {
        Simulation sim = new Simulation(SimOptions.fromSeed(seed));
        sim.run(8_000);
        sim.setDelays(1, 3, 2);
        sim.setPauses(false);
        sim.run(4_000);
        long before = sim.stats().appliedCommands;
        sim.run(1_000);
        assertTrue(sim.stats().appliedCommands > before,
                "no progress after the network became calm: " + sim.stats());
        sim.setWorkload(false);
        // A narrow flow-control window can leave a long backlog; working it off takes time
        // in proportion to its length, but it must shrink.
        for (int i = 0; i < 240 && !sim.isConverged(); i++) {
            sim.run(500);
        }
        sim.checkConverged();
        assertEquals(0, sim.countUndecidedProposals(), sim.stats().toString());
        return sim;
    }

    @Test
    void randomClustersStaySafeAndConverge() {
        long[] totals = new long[8];
        Seeds.forEach(RaftSimulationTest.class, "raft-sim-reliable", 200, seed -> {
            Simulation sim = runSeed(seed);
            Simulation.Stats stats = sim.stats();
            totals[0]++;
            totals[1] += sim.checks().leaderOfTerm().size();
            totals[2] += stats.truncations;
            totals[3] += stats.rejectedOverloaded;
            totals[4] += stats.batchedWrites;
            totals[5] += stats.lost;
            totals[6] += stats.rejectedNotLeader;
            totals[7] += stats.appliedCommands;
        });
        if (totals[0] >= 100) {
            // The seed set as a whole must reach the interesting situations.
            String summary = java.util.Arrays.toString(totals);
            assertTrue(totals[1] > 2 * totals[0], "leader changes: " + summary);
            assertTrue(totals[2] > 0, "log truncations: " + summary);
            assertTrue(totals[3] > 0, "overload rejections: " + summary);
            assertTrue(totals[4] > 0, "writes covered by one completion: " + summary);
            assertTrue(totals[5] > 0, "accepted proposals that were lost: " + summary);
            assertTrue(totals[6] > 0, "proposals to a node that is not the leader: " + summary);
            assertTrue(totals[7] > 100 * totals[0], "applied commands: " + summary);
        }
    }

    @Test
    void aRunIsAFunctionOfItsSeed() {
        long first = runSeed(7).traceHash();
        assertEquals(first, runSeed(7).traceHash());
        assertNotEquals(first, runSeed(8).traceHash());
    }
}
