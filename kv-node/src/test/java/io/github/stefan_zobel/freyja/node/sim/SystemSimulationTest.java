package io.github.stefan_zobel.freyja.node.sim;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.common.testing.Seeds;

/**
 * The whole-system simulation over its seed sets (docs/spec/verification.md, section 3). A
 * set must also reach the situations it is there for (section 3.1): a fault that never
 * happens proves nothing.
 */
class SystemSimulationTest {

    @Test
    void aCalmClusterServesItsClients() {
        SystemSimulation.Stats total = new SystemSimulation.Stats();
        Seeds.forEach(SystemSimulationTest.class, "system-calm", 3, seed ->
                total.add(new SystemSimulation(seed, SystemSimulation.Options.calm()).run()));
        assertTrue(total.acknowledged > 300, total.toString());
        assertTrue(total.reads > 200, total.toString());
        assertTrue(total.casHeld > 0 && total.casFailed > 0, total.toString());
        assertTrue(total.snapshotsCreated > 0, total.toString());
        assertTrue(total.givenUp == 0 && total.crashes == 0, total.toString());
    }

    @Test
    void aClusterWithNetworkFaultsAndCrashesKeepsItsPromises() {
        SystemSimulation.Stats total = new SystemSimulation.Stats();
        Seeds.forEach(SystemSimulationTest.class, "system-faulty", 12, seed ->
                total.add(new SystemSimulation(seed, SystemSimulation.Options.faulty()).run()));
        assertTrue(total.acknowledged > 500, total.toString());
        assertTrue(total.reads > 200, total.toString());
        assertTrue(total.casHeld > 0 && total.casFailed > 0, total.toString());
        assertTrue(total.retries > 0, total.toString());
        assertTrue(total.lateDuplicates > 100, total.toString());
        assertTrue(total.leaderChanges > 12, total.toString());
        assertTrue(total.crashes > 12, total.toString());
        assertTrue(total.crashesInAnOperation > 3, total.toString());
        assertTrue(total.partitions > 5, total.toString());
        assertTrue(total.dropped > 0 && total.duplicated > 0, total.toString());
        assertTrue(total.snapshotsCreated > 0, total.toString());
    }
}
