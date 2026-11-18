package io.github.sz.freyja.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.lsm.CorruptionException;
import io.github.sz.freyja.lsm.fs.FaultyFileSystem;
import io.github.sz.freyja.node.KvCluster.Member;
import io.github.sz.freyja.node.sm.StateMachineStore;
import io.github.sz.freyja.node.store.Bootstrap;
import io.github.sz.freyja.node.store.HardStateStore;
import io.github.sz.freyja.node.store.RepairMarker;
import io.github.sz.freyja.raft.HardState;

/**
 * Repair of a damaged state machine (docs/spec/node.md, section 6): the node gives the state
 * machine up and gets its content back, from its own log or from the leader.
 */
class RepairTest {

    private static final Path SM = KvCluster.DATA.resolve(StateMachineStore.DIRECTORY);

    private static List<String> names(Member member, Path directory) throws IOException {
        return member.memory.list(directory).stream().map(p -> p.getFileName().toString()).toList();
    }

    /** Flips a bit in every table of the state machine, at an offset inside a block. */
    private static int damageTables(Member member, long offset, long minimumSize) throws IOException {
        int damaged = 0;
        for (String generation : names(member, SM)) {
            if (!generation.matches("\\d{10}")) {
                continue;
            }
            for (Path path : member.memory.list(SM.resolve(generation))) {
                if (path.getFileName().toString().endsWith(".sst") && member.memory.size(path) >= minimumSize) {
                    byte[] one = member.memory.peek(path, offset, 1);
                    one[0] ^= 0x40;
                    member.memory.overwrite(path, offset, one);
                    damaged++;
                }
            }
        }
        return damaged;
    }

    private static void assertRepaired(KvCluster cluster, Member member) throws IOException {
        assertFalse(RepairMarker.exists(member.memory, KvCluster.DATA));
        assertEquals(2, names(member, SM).size(), "one generation and CURRENT: " + names(member, SM));
        assertFalse(member.node.view().logRecovering());
        cluster.assertSameState();
    }

    @Test
    void aStateMachineThatDoesNotOpenIsRebuiltFromTheLog() throws IOException {
        // The logs are never shortened: they reach back to the first entry.
        KvCluster cluster = KvCluster.ofThree(1L << 30);
        Random random = new Random(1);
        cluster.write(60, 300, random);
        Member victim = cluster.members.get((cluster.leader().id + 1) % 3);
        victim.node.store().flush();
        long epoch = victim.node.view().logEpoch();
        victim.stop();
        assertTrue(damageTables(victim, 50, 0) > 0);

        victim.start();
        assertTrue(victim.node.repairedAtStart());
        assertEquals(2, victim.node.store().generation());
        assertFalse(victim.node.view().logRecovering(), "the log is whole: nothing was promised and lost");
        assertEquals(epoch, victim.node.view().logEpoch());
        cluster.ticks(10);
        assertEquals(0, victim.node.snapshotsInstalled(), "its own log was enough");
        assertRepaired(cluster, victim);

        victim.stop();
        victim.start();
        assertFalse(victim.node.repairedAtStart());
        cluster.write(10, 100, random);
        cluster.assertSameState();
        cluster.close();
    }

    @Test
    void withAShortenedLogTheStateMachineComesFromTheLeader() throws IOException {
        KvCluster cluster = KvCluster.ofThree();
        Random random = new Random(2);
        cluster.write(120, 300, random);
        Member victim = cluster.members.get((cluster.leader().id + 1) % 3);
        assertTrue(victim.node.firstStoredIndex() > 1);
        long epoch = victim.node.view().logEpoch();
        long last = victim.node.view().lastLogIndex();
        victim.stop();
        assertTrue(damageTables(victim, 50, 0) > 0);

        victim.start();
        assertTrue(victim.node.repairedAtStart());
        assertTrue(victim.node.view().logRecovering(), "the node acknowledged entries it no longer holds");
        assertEquals(epoch + 1, victim.node.view().logEpoch());
        assertEquals(0, victim.node.view().lastLogIndex());
        assertEquals(0, victim.node.snapshot().lastIncludedIndex());
        HardState hard = HardStateStore.open(victim.memory, KvCluster.DATA).get();
        assertTrue(hard.logRecovering());
        assertEquals(last, hard.lostLogIndex(), "what the lost log held is known: the node can still vote");
        assertTrue(hard.lostLogTerm() != HardState.UNKNOWN_TERM);
        assertFalse(RepairMarker.exists(victim.memory, KvCluster.DATA));

        cluster.ticks(120);
        assertEquals(1, victim.node.snapshotsInstalled());
        assertRepaired(cluster, victim);
        cluster.write(10, 100, random);
        cluster.assertSameState();
        cluster.close();
    }

    @Test
    void aLogTailThatIsLostWithoutATraceIsWitnessedByTheStateMachine() throws IOException {
        KvCluster cluster = KvCluster.ofThree(1L << 30);
        Random random = new Random(9);
        cluster.write(40, 300, random);
        Member victim = cluster.members.get((cluster.leader().id + 1) % 3);
        long applied = victim.applied();
        long epoch = victim.node.view().logEpoch();
        victim.stop();
        // Damage in the last record of the newest segment, after the last sync point: the
        // format cannot tell it from a write that a crash tore (storage-format.md, 3.2).
        Path raft = KvCluster.DATA.resolve(Node.RAFT_DIRECTORY);
        Path newest = null;
        for (Path path : victim.memory.list(raft)) {
            if (path.getFileName().toString().endsWith(".rlog") && victim.memory.size(path) > 400) {
                newest = path;
            }
        }
        long size = victim.memory.size(newest);
        byte[] one = victim.memory.peek(newest, size - 20, 1);
        one[0] ^= 0x02;
        victim.memory.overwrite(newest, size - 20, one);

        victim.start();
        assertTrue(victim.node.view().lastLogIndex() < applied, "the log lost its end without a sign");
        assertTrue(victim.node.lostTailWitnessed(), "but the state machine had applied those entries");
        assertTrue(victim.node.view().logRecovering());
        assertEquals(epoch + 1, victim.node.view().logEpoch());
        assertTrue(HardStateStore.open(victim.memory, KvCluster.DATA).get().logRecovering());
        assertEquals(applied, victim.applied(), "what was applied stays applied");

        cluster.ticks(60);
        assertFalse(victim.node.view().logRecovering());
        cluster.write(10, 100, random);
        cluster.assertSameState();
        victim.stop();
        victim.start();
        assertFalse(victim.node.lostTailWitnessed());
        cluster.close();
    }

    @Test
    void damageFoundByAScrubStopsTheNodeAndIsRepairedAtTheNextStart() throws IOException {
        KvCluster cluster = KvCluster.ofThree();
        Random random = new Random(3);
        cluster.write(120, 300, random);
        Member victim = cluster.members.get((cluster.leader().id + 1) % 3);
        // A block in the middle of a table: nothing reads it until a scrub comes by.
        assertTrue(damageTables(victim, 6_000, 12_000) > 0);
        cluster.write(5, 100, random);
        assertTrue(victim.running(), "nobody has noticed yet");
        assertTrue(victim.node.store().machine().engine().scrub() > 0);
        cluster.ticks(1);
        assertFalse(victim.running());
        assertEquals(List.of(victim), cluster.failed);
        assertTrue(RepairMarker.exists(victim.memory, KvCluster.DATA));

        victim.start();
        assertTrue(victim.node.repairedAtStart());
        cluster.ticks(120);
        assertRepaired(cluster, victim);
        cluster.close();
    }

    @Test
    void aRepairInterruptedAtAnyPointIsDoneAgainAtTheNextStart() throws IOException {
        int interrupted = 0;
        for (int failAt = 0; failAt < 400; failAt++) {
            KvCluster cluster = KvCluster.ofThree();
            Random random = new Random(200 + failAt);
            cluster.write(80, 300, random);
            Member victim = cluster.members.get((cluster.leader().id + 1) % 3);
            assertTrue(victim.node.firstStoredIndex() > 1);
            victim.stop();
            assertTrue(damageTables(victim, 50, 0) > 0);

            FaultyFileSystem faulty = new FaultyFileSystem(victim.memory);
            int at = failAt;
            long[] count = {-1};
            faulty.setFault((operation, path, index) -> {
                // Counting starts when the marker is written: the repair has begun.
                if (count[0] == -1 && path.getFileName().toString().startsWith(RepairMarker.FILE_NAME)) {
                    count[0] = 0;
                }
                return count[0] != -1 && count[0]++ == at ? new IOException("injected") : null;
            });
            victim.fs = faulty;
            boolean failed = false;
            try {
                victim.start();
            } catch (IOException e) {
                assertEquals("injected", e.getMessage());
                failed = true;
            }
            if (!failed) {
                // The fault lay beyond the last file operation of the start.
                faulty.setFault(null);
                assertTrue(failAt > 15, "file operations of a repair: " + failAt);
                cluster.ticks(120);
                assertRepaired(cluster, victim);
                cluster.close();
                break;
            }
            interrupted++;
            victim.memory.crash(random);
            victim.fs = victim.memory;
            victim.start();
            assertTrue(victim.node.view().logRecovering());
            cluster.ticks(120);
            assertEquals(1, victim.node.snapshotsInstalled());
            assertRepaired(cluster, victim);
            cluster.close();
        }
        assertTrue(interrupted > 15, "interrupted repairs: " + interrupted);
    }

    @Test
    void withoutTheBootstrapFileTheMemberMustBeReplaced() throws IOException {
        KvCluster cluster = KvCluster.ofThree();
        Random random = new Random(4);
        cluster.write(20, 100, random);
        Member victim = cluster.members.get((cluster.leader().id + 1) % 3);
        victim.node.store().flush();
        victim.stop();
        assertTrue(damageTables(victim, 50, 0) > 0);
        Path bootstrap = KvCluster.DATA.resolve(Bootstrap.FILE_NAME);
        byte[] one = victim.memory.peek(bootstrap, 6, 1);
        one[0] ^= 1;
        victim.memory.overwrite(bootstrap, 6, one);
        IOException refused = assertThrows(IOException.class, victim::start);
        assertFalse(refused instanceof CorruptionException);
        assertTrue(refused.getMessage().contains("Replace the member"), refused.getMessage());
        assertTrue(RepairMarker.exists(victim.memory, KvCluster.DATA), "the damaged state machine stays given up");
        assertEquals(0, victim.memory.openHandleCount());
        victim.node = null;
        cluster.close();
    }
}
