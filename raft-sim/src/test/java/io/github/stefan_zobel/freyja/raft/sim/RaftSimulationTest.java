package io.github.stefan_zobel.freyja.raft.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.common.testing.Seeds;
import io.github.stefan_zobel.freyja.raft.sim.LinearizabilityChecker.Operation;

/**
 * Simulated clusters (docs/spec/verification.md, sections 2.4 and 2.5). The safety invariants
 * are checked by the simulation after every step. The tests add liveness after the faults
 * have ended, and linearizability of the client history.
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
            assertEquals(1, sim.observation().leadersOfTerm().size(), "one election: " + stats);
            assertTrue(stats.accepted > 300, stats.toString());
            assertEquals(stats.accepted, stats.appliedCommands, stats.toString());
            assertEquals(0, stats.truncations);
            // The entry of the leader's term plus every command.
            assertEquals(stats.accepted + 1, sim.observation().appliedLog().size());
            List<Operation> history = sim.checkedHistory();
            assertTrue(history.size() > 40, "operations in the checked history: " + history.size());
            assertNull(LinearizabilityChecker.check(history));
            assertTrue(stats.readsServed > 10, stats.toString());
        }
    }

    /**
     * Lets the cluster work off its backlog. A narrow flow-control window can leave a long
     * one; that takes time in proportion to its length, but it must end.
     */
    private static void converge(Simulation sim) {
        for (int i = 0; i < 240 && !sim.isConverged(); i++) {
            sim.run(500);
        }
        if (!sim.isConverged()) {
            String problem;
            try {
                sim.checkConverged();
                problem = "";
            } catch (AssertionError e) {
                problem = e.getMessage();
            }
            throw new AssertionError("not converged: " + problem + " | " + sim.stats() + " " + sim.options() + " "
                    + sim.nodes().stream().map(n -> n.view() + " applied=" + n.applied()).toList());
        }
    }

    /**
     * Runs one seed: a rough phase, then a calm one in which the cluster must converge and
     * commit new proposals.
     */
    private static Simulation runSeed(SimOptions options) {
        Simulation sim = new Simulation(options);
        sim.run(8_000);
        sim.calm();
        sim.setWorkload(false);
        converge(sim);

        long before = sim.stats().appliedCommands;
        sim.setWorkload(true);
        sim.run(1_000);
        sim.setWorkload(false);
        converge(sim);
        assertTrue(sim.stats().appliedCommands > before,
                "no new proposal was committed after the faults ended: " + sim.stats());
        sim.checkConverged();
        assertEquals(0, sim.countUndecidedProposals(), sim.stats().toString());
        assertNull(LinearizabilityChecker.check(sim.checkedHistory()));
        return sim;
    }

    /** What a seed set reached, summed over its runs. */
    private static final class Totals {
        long runs;
        long elections;
        long truncations;
        long overloaded;
        long batchedWrites;
        long lost;
        long notLeader;
        long applied;
        long crashes;
        long partitions;
        long dropped;
        long duplicated;
        long readsServed;
        long historyOperations;
        long configChanges;
        long joined;
        long retired;
        long transfers;
        long transfersAborted;
        long logDamages;
        long logRecoveries;

        void add(Simulation sim) {
            Simulation.Stats stats = sim.stats();
            runs++;
            elections += sim.observation().leadersOfTerm().size();
            truncations += stats.truncations;
            overloaded += stats.rejectedOverloaded;
            batchedWrites += stats.batchedWrites;
            lost += stats.lost;
            notLeader += stats.rejectedNotLeader;
            applied += stats.appliedCommands;
            crashes += stats.crashes;
            partitions += stats.partitions;
            dropped += stats.dropped;
            duplicated += stats.duplicated;
            readsServed += stats.readsServed;
            historyOperations += sim.checkedHistory().size();
            configChanges += stats.configChanges;
            joined += stats.joined;
            retired += stats.retired;
            transfers += stats.transfers;
            transfersAborted += stats.transfersAborted;
            logDamages += stats.logDamages;
            logRecoveries += stats.logRecoveries;
        }

        @Override
        public String toString() {
            return "runs=" + runs + " elections=" + elections + " truncations=" + truncations
                    + " overloaded=" + overloaded + " batchedWrites=" + batchedWrites + " lost=" + lost
                    + " notLeader=" + notLeader + " applied=" + applied + " crashes=" + crashes
                    + " partitions=" + partitions + " dropped=" + dropped + " duplicated=" + duplicated
                    + " readsServed=" + readsServed + " historyOperations=" + historyOperations
                    + " configChanges=" + configChanges + " joined=" + joined + " retired=" + retired
                    + " transfers=" + transfers + " transfersAborted=" + transfersAborted
                    + " logDamages=" + logDamages + " logRecoveries=" + logRecoveries;
        }
    }

    @Test
    void clustersOnAReliableNetworkStaySafeAndConverge() {
        Totals totals = new Totals();
        Seeds.forEach(RaftSimulationTest.class, "raft-sim-reliable", 60,
                seed -> totals.add(runSeed(SimOptions.reliable(seed))));
        System.out.println("reliable network: " + totals);
        if (totals.runs >= 60) {
            // The seed set as a whole must reach the interesting situations.
            String summary = totals.toString();
            assertTrue(totals.elections > 2 * totals.runs, summary);
            assertTrue(totals.truncations > 0, summary);
            assertTrue(totals.overloaded > 0, summary);
            assertTrue(totals.batchedWrites > 0, summary);
            assertTrue(totals.lost > 0, summary);
            assertTrue(totals.notLeader > 0, summary);
            assertTrue(totals.applied > 100 * totals.runs, summary);
            assertTrue(totals.readsServed > 10 * totals.runs, summary);
            assertTrue(totals.historyOperations > 50 * totals.runs, summary);
            assertEquals(0, totals.crashes + totals.partitions + totals.dropped + totals.duplicated, summary);
        }
    }

    @Test
    void clustersUnderTheFullFaultModelStaySafeAndConverge() {
        Totals totals = new Totals();
        Seeds.forEach(RaftSimulationTest.class, "raft-sim-faulty", 200,
                seed -> totals.add(runSeed(SimOptions.faulty(seed))));
        System.out.println("full fault model: " + totals);
        if (totals.runs >= 100) {
            String summary = totals.toString();
            assertTrue(totals.elections > 3 * totals.runs, summary);
            assertTrue(totals.truncations > totals.runs / 4, summary);
            assertTrue(totals.lost > 0, summary);
            assertTrue(totals.crashes > totals.runs, summary);
            assertTrue(totals.partitions > totals.runs / 2, summary);
            assertTrue(totals.dropped > 100 * totals.runs, summary);
            assertTrue(totals.duplicated > 10 * totals.runs, summary);
            assertTrue(totals.readsServed > 5 * totals.runs, summary);
            assertTrue(totals.historyOperations > 30 * totals.runs, summary);
        }
    }

    @Test
    void clustersWhoseMembershipChangesStaySafeAndConverge() {
        Totals totals = new Totals();
        Seeds.forEach(RaftSimulationTest.class, "raft-sim-membership", 150,
                seed -> totals.add(runSeed(SimOptions.reconfiguring(seed))));
        System.out.println("membership changes: " + totals);
        if (totals.runs >= 100) {
            String summary = totals.toString();
            assertTrue(totals.configChanges > 4 * totals.runs, summary);
            assertTrue(totals.joined > 2 * totals.runs, summary);
            assertTrue(totals.retired > totals.runs / 2, summary);
            assertTrue(totals.transfers > totals.runs / 2, summary);
            assertTrue(totals.transfersAborted > 0, summary);
            assertTrue(totals.logDamages > totals.runs / 2, summary);
            assertTrue(totals.logRecoveries > totals.runs / 4, summary);
            assertTrue(totals.elections > 3 * totals.runs, summary);
            assertTrue(totals.truncations > 0, summary);
            assertTrue(totals.crashes > totals.runs / 2, summary);
            assertTrue(totals.historyOperations > 20 * totals.runs, summary);
        }
    }

    @Test
    void aRunIsAFunctionOfItsSeed() {
        long first = runSeed(SimOptions.faulty(7)).traceHash();
        assertEquals(first, runSeed(SimOptions.faulty(7)).traceHash());
        assertNotEquals(first, runSeed(SimOptions.faulty(8)).traceHash());
    }
}
