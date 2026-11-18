package io.github.sz.freyja.node.sim;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.common.testing.Seeds;

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
        System.out.println("system-calm: " + total);
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
        System.out.println("system-faulty: " + total);
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
        assertTrue(total.tornTailsDropped > 12 && total.writesHeldBack > 12, total.toString());
    }

    @Test
    void aClusterWithDamagedFilesAndFullAndSlowDisksRepairsItself() {
        SystemSimulation.Stats total = new SystemSimulation.Stats();
        Seeds.forEach(SystemSimulationTest.class, "system-hostile", 12, seed ->
                total.add(new SystemSimulation(seed, SystemSimulation.Options.hostile()).run()));
        System.out.println("system-hostile: " + total);
        assertTrue(total.sessionsLostInRetry > 0, total.toString());
        assertTrue(total.transfersAborted > 0, total.toString());
        assertTrue(total.diskFullStops > 0, total.toString());
        assertTrue(total.acknowledged > 500, total.toString());
        assertTrue(total.damagedLogs > 3 && total.damagedTables > 3, total.toString());
        assertTrue(total.recoveringEntered > 3, total.toString());
        assertTrue(total.recoveringLeft > 3, total.toString());
        assertTrue(total.repairs > 0, total.toString());
        assertTrue(total.snapshotsInstalled > 0, total.toString());
        assertTrue(total.disksShort > 3 && total.rejectedForSpace > 0 && total.alarmsCleared > 0, total.toString());
        assertTrue(total.slowDisks > 3 && total.slowDiskStepDowns > 0, total.toString());
        assertTrue(total.transfers > 3, total.toString());
        assertTrue(total.tornTailsDropped > 12 && total.writesHeldBack > 12, total.toString());
        assertTrue(total.damageProvenBySyncPoint > 0, total.toString());
    }

    @Test
    void aClusterWhoseMembersAreReplacedStaysOneCluster() {
        SystemSimulation.Stats total = new SystemSimulation.Stats();
        Seeds.forEach(SystemSimulationTest.class, "system-changing", 12, seed ->
                total.add(new SystemSimulation(seed, SystemSimulation.Options.changing()).run()));
        System.out.println("system-changing: " + total);
        assertTrue(total.configurationsReverted > 0, total.toString());
        assertTrue(total.installsAroundALeaderChange > 0, total.toString());
        assertTrue(total.acknowledged > 500, total.toString());
        assertTrue(total.replacements > 12, total.toString());
        assertTrue(total.identitiesLost > 3, total.toString());
        assertTrue(total.snapshotsInstalled > 12, total.toString());
        assertTrue(total.recoveringEntered > 0 && total.repairs > 0, total.toString());
    }
}
