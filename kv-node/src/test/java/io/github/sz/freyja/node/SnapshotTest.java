package io.github.sz.freyja.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.common.testing.Seeds;
import io.github.sz.freyja.lsm.fs.FaultyFileSystem;
import io.github.sz.freyja.node.KvCluster.Member;
import io.github.sz.freyja.node.sm.StateMachineStore;
import io.github.sz.freyja.node.snapshot.SnapshotTransfers;
import io.github.sz.freyja.node.store.InstallIntent;
import io.github.sz.freyja.node.store.SnapshotMetaFile;
import io.github.sz.freyja.node.wire.TransferCodec;
import io.github.sz.freyja.node.wire.TransferMessage;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.ConfigChange;
import io.github.sz.freyja.raft.RaftEvent;
import io.github.sz.freyja.raft.SnapshotMeta;

/**
 * Snapshots with complete nodes (docs/spec/node.md, section 5): logs are shortened, a node
 * that is too far behind receives the state machine of the leader, and an installation that a
 * crash interrupts is completed.
 */
class SnapshotTest {

    private static final Path SM = KvCluster.DATA.resolve(StateMachineStore.DIRECTORY);

    private static List<String> names(Member member, Path directory) throws IOException {
        return member.memory.list(directory).stream().map(p -> p.getFileName().toString()).toList();
    }

    /** A follower that stopped, and the others moving on until their logs no longer reach back. */
    private static Member leaveBehind(KvCluster cluster, Random random) throws IOException {
        cluster.write(10, 200, random);
        Member leader = cluster.leader();
        Member behind = cluster.members.get((leader.id + 1) % 3);
        behind.stop();
        cluster.write(120, 300, random);
        for (Member member : cluster.members) {
            if (member.running()) {
                assertTrue(member.node.snapshotsCreated() > 0, "node " + member.id + " shortened its log");
                assertTrue(member.node.firstStoredIndex() > 20, "first index " + member.node.firstStoredIndex());
                assertEquals(member.node.snapshot(), SnapshotMetaFile.read(member.memory, KvCluster.DATA));
            }
        }
        return behind;
    }

    @Test
    void theLogIsShortenedAndTheClusterRestartsFromWhatIsLeft() throws IOException {
        KvCluster cluster = KvCluster.ofThree();
        Random random = new Random(1);
        cluster.write(150, 300, random);
        long applied = cluster.leader().applied();
        for (Member member : cluster.members) {
            assertTrue(member.node.snapshotsCreated() > 0);
            assertTrue(member.node.logSegmentCount() < 12, "segments: " + member.node.logSegmentCount());
            SnapshotMeta snapshot = member.node.snapshot();
            assertTrue(snapshot.lastIncludedIndex() <= applied - 5, "five entries are kept: " + snapshot);
            assertTrue(member.node.firstStoredIndex() <= snapshot.lastIncludedIndex() + 1);
            assertEquals(0, member.node.snapshotsInstalled());
        }
        // On request the log is shortened as far as it goes.
        Member one = cluster.members.get(0);
        assertTrue(one.node.requestSnapshot());
        cluster.settle();
        assertEquals(one.applied(), one.node.snapshot().lastIncludedIndex());

        // All nodes stop, one of them by a crash, and start from what is on disk.
        long digest = one.digest();
        cluster.members.get(1).crash(random);
        cluster.members.get(0).stop();
        cluster.members.get(2).stop();
        for (Member member : cluster.members) {
            member.start();
        }
        cluster.write(20, 100, random);
        cluster.assertSameState();
        assertTrue(cluster.leader().applied() > applied);
        assertTrue(digest != one.digest());
        for (Member member : cluster.members) {
            assertEquals(0, member.node.snapshotsInstalled(), "nobody needed the state of another node");
        }
        cluster.close();
    }

    @Test
    void aNodeThatIsTooFarBehindReceivesTheStateMachine() throws IOException {
        KvCluster cluster = KvCluster.ofThree();
        Random random = new Random(2);
        Member behind = leaveBehind(cluster, random);
        behind.start();
        cluster.ticks(60);

        assertEquals(1, behind.node.snapshotsInstalled());
        assertEquals(1, behind.installed.size());
        assertEquals(2, behind.node.store().generation());
        assertEquals(List.of("0000000002", "CURRENT"), names(behind, SM), "the old generation is gone");
        assertTrue(behind.node.firstStoredIndex() > 20);
        assertNotNull(SnapshotMetaFile.read(behind.memory, KvCluster.DATA));
        assertFalse(behind.memory.exists(KvCluster.DATA.resolve(InstallIntent.FILE_NAME)));
        cluster.assertSameState();
        Member leader = cluster.leader();
        assertEquals(1, leader.node.transfers().sentCount());
        assertEquals(0, leader.node.transfers().outgoingCount());
        assertEquals(List.of(), names(leader, KvCluster.DATA.resolve(SnapshotTransfers.DIRECTORY)),
                "the checkpoint that was sent is removed");

        // The node goes on as a normal follower, and survives a restart.
        cluster.write(20, 100, random);
        behind.stop();
        behind.start();
        cluster.write(5, 100, random);
        cluster.assertSameState();
        assertEquals(0, behind.node.snapshotsInstalled(), "counted since the start");
        cluster.close();
    }

    @Test
    void aNewNodeJoinsThroughASnapshot() throws IOException {
        KvCluster cluster = KvCluster.ofThree();
        Random random = new Random(3);
        cluster.write(150, 300, random);
        Member joiner = cluster.add(ClusterConfig.EMPTY);
        joiner.start();
        cluster.leader().node.submit(new RaftEvent.ProposeConfigChange(1_000_000, new ConfigChange.AddLearner(3)));
        cluster.ticks(80);
        assertEquals(1, joiner.node.snapshotsInstalled());
        assertTrue(joiner.node.view().config().learners().contains(3), joiner.node.view().config().toString());
        assertEquals(cluster.leader().node.store().machine().config(), joiner.node.store().machine().config());
        cluster.assertSameState();

        cluster.leader().node.submit(new RaftEvent.ProposeConfigChange(1_000_001,
                new ConfigChange.PromoteLearner(3)));
        cluster.ticks(10);
        cluster.write(10, 100, random);
        cluster.assertSameState();
        assertTrue(joiner.node.view().config().voters().contains(3));
        cluster.close();
    }

    @Test
    void aTransferSurvivesLostAndRepeatedMessages() {
        Seeds.forEach(SnapshotTest.class, "snapshot-transfer", 8, seed -> {
            KvCluster cluster = KvCluster.ofThree();
            Random random = new Random(seed);
            Member behind = leaveBehind(cluster, random);
            // A fifth of the messages of the transfer is lost, in both directions.
            long[] lost = {0};
            cluster.lose = frame -> {
                if (TransferCodec.isTransfer(frame) && random.nextInt(5) == 0) {
                    lost[0]++;
                    return true;
                }
                return false;
            };
            // Another fifth arrives twice.
            cluster.duplicate = frame -> TransferCodec.isTransfer(frame) && random.nextInt(5) == 0;
            behind.start();
            for (int i = 0; i < 600 && behind.node.snapshotsInstalled() == 0; i++) {
                cluster.ticks(1);
            }
            assertEquals(1, behind.node.snapshotsInstalled(), "lost messages: " + lost[0]);
            assertTrue(lost[0] > 3, "lost messages: " + lost[0]);
            cluster.lose = null;
            cluster.duplicate = null;
            assertEquals(0, behind.node.transfers().failedCount(), "no transfer had to start again");
            cluster.ticks(50);
            cluster.assertSameState();
            for (Member member : cluster.members) {
                assertEquals(0, member.node.transfers().outgoingCount());
                assertFalse(member.node.transfers().receiving());
            }
            cluster.close();
        });
    }

    @Test
    void aTransferWithoutAnswerIsGivenUpAndStartedAgain() throws IOException {
        KvCluster cluster = KvCluster.ofThree();
        Random random = new Random(5);
        Member behind = leaveBehind(cluster, random);
        Member leader = cluster.leader();
        // The node is reachable for the core but every chunk is lost.
        cluster.lose = frame -> TransferCodec.isTransfer(frame)
                && TransferCodec.decode(frame).message() instanceof TransferMessage.Chunk;
        behind.start();
        cluster.ticks(30);
        assertEquals(1, leader.node.transfers().outgoingCount());
        assertTrue(behind.node.transfers().receiving());
        assertEquals(List.of("0000000001", "0000000002", "CURRENT"), names(behind, SM));
        for (int i = 0; i < 100 && leader.node.transfers().failedCount() == 0; i++) {
            cluster.ticks(1);
        }
        assertTrue(leader.node.transfers().failedCount() > 0, "the leader gave the first transfer up");
        assertTrue(behind.node.transfers().failedCount() > 0);
        assertEquals(0, behind.node.snapshotsInstalled());

        cluster.lose = null;
        cluster.ticks(80);
        assertEquals(1, behind.node.snapshotsInstalled());
        cluster.assertSameState();
        assertEquals(2, names(behind, SM).size(), "one generation and CURRENT: " + names(behind, SM));
        assertEquals(List.of(), names(leader, KvCluster.DATA.resolve(SnapshotTransfers.DIRECTORY)));
        cluster.close();
    }

    @Test
    void aSnapshotThatArrivesDamagedIsNotInstalled() throws IOException {
        KvCluster cluster = KvCluster.ofThree();
        Random random = new Random(6);
        Member behind = leaveBehind(cluster, random);
        // While the transfer runs, a byte in the middle of a received table goes bad: in a
        // block that nobody reads when the state machine is opened.
        boolean[] done = {false};
        cluster.lose = frame -> {
            if (!done[0] && TransferCodec.isTransfer(frame)
                    && TransferCodec.decode(frame).message() instanceof TransferMessage.Chunk) {
                try {
                    for (Path path : behind.memory.list(SM.resolve("0000000002"))) {
                        long size = behind.memory.size(path);
                        if (path.getFileName().toString().endsWith(".sst") && size > 12_000) {
                            // Not the first block, which holds the system keys.
                            byte[] one = behind.memory.peek(path, 6_000, 1);
                            one[0] ^= 0x10;
                            behind.memory.overwrite(path, 6_000, one);
                            done[0] = true;
                            break;
                        }
                    }
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            return false;
        };
        behind.start();
        for (int i = 0; i < 300 && behind.node.snapshotsInstalled() == 0; i++) {
            cluster.ticks(1);
        }
        assertTrue(done[0], "a received table was damaged");
        assertEquals(1, behind.node.snapshotsInstalled(), "the second transfer was intact");
        assertTrue(behind.node.transfers().failedCount() >= 1, "the first one was found damaged");
        assertEquals(1, behind.node.transfers().receivedCount());
        cluster.ticks(20);
        cluster.assertSameState();
        cluster.close();
    }

    @Test
    void anInstallationInterruptedAtAnyPointIsCompletedAtTheNextStart() throws IOException {
        int completedByRestart = 0;
        for (int failAt = 0; failAt < 200; failAt++) {
            KvCluster cluster = KvCluster.ofThree();
            Random random = new Random(100 + failAt);
            Member behind = leaveBehind(cluster, random);
            FaultyFileSystem faulty = new FaultyFileSystem(behind.memory);
            int at = failAt;
            long[] count = {-1};
            faulty.setFault((operation, path, index) -> {
                // Counting starts when the intent is written: the installation has begun.
                if (count[0] == -1 && path.getFileName().toString().startsWith(InstallIntent.FILE_NAME)) {
                    count[0] = 0;
                }
                return count[0] != -1 && count[0]++ == at ? new IOException("injected") : null;
            });
            behind.fs = faulty;
            behind.start();
            for (int i = 0; i < 200 && behind.running() && behind.node.snapshotsInstalled() == 0; i++) {
                cluster.ticks(1);
            }
            if (behind.running()) {
                // The fault lay beyond the last step of the installation.
                assertEquals(1, behind.node.snapshotsInstalled());
                assertTrue(failAt > 8, "steps of an installation: " + failAt);
                cluster.close();
                break;
            }
            assertEquals(List.of(behind), cluster.failed);
            boolean intent = behind.memory.exists(KvCluster.DATA.resolve(InstallIntent.FILE_NAME));
            behind.memory.crash(random);
            behind.fs = behind.memory;
            behind.start();
            assertFalse(behind.memory.exists(KvCluster.DATA.resolve(InstallIntent.FILE_NAME)));
            if (intent && behind.memory.exists(KvCluster.DATA.resolve(InstallIntent.FILE_NAME))) {
                throw new AssertionError("unreachable");
            }
            if (behind.node.store().generation() == 2 && behind.node.snapshotsInstalled() == 0
                    && behind.applied() > 20) {
                completedByRestart++;
            }
            assertEquals(2, names(behind, SM).size(), "one generation and CURRENT: " + names(behind, SM));
            cluster.ticks(120);
            cluster.assertSameState();
            cluster.write(5, 100, random);
            cluster.assertSameState();
            cluster.close();
        }
        assertTrue(completedByRestart > 3, "installations completed by a restart: " + completedByRestart);
    }

    @Test
    void theFileThatSaysWhereTheLogStartsCanBeLost() throws IOException {
        KvCluster cluster = KvCluster.ofThree();
        Random random = new Random(7);
        cluster.write(150, 300, random);
        Member member = cluster.members.get((cluster.leader().id + 1) % 3);
        SnapshotMeta before = member.node.snapshot();
        long applied = member.applied();
        member.stop();
        Path file = KvCluster.DATA.resolve(SnapshotMetaFile.FILE_NAME);
        byte[] one = member.memory.peek(file, 7, 1);
        one[0] ^= 1;
        member.memory.overwrite(file, 7, one);
        member.start();
        // The state machine stands in: it holds everything the log gave up.
        assertEquals(applied, member.node.snapshot().lastIncludedIndex());
        assertTrue(member.node.snapshot().lastIncludedIndex() >= before.lastIncludedIndex());
        assertEquals(member.node.snapshot(), SnapshotMetaFile.read(member.memory, KvCluster.DATA));
        cluster.write(20, 100, random);
        cluster.assertSameState();
        cluster.close();
    }

    @Test
    void aDamagedLogBehindASnapshotIsRefilledByTheLeader() throws IOException {
        KvCluster cluster = KvCluster.ofThree();
        Random random = new Random(8);
        cluster.write(150, 300, random);
        Member victim = cluster.members.get((cluster.leader().id + 1) % 3);
        long epoch = victim.node.view().logEpoch();
        victim.stop();
        Path raft = KvCluster.DATA.resolve(Node.RAFT_DIRECTORY);
        List<Path> segments = victim.memory.list(raft).stream()
                .filter(p -> p.getFileName().toString().endsWith(".rlog")).sorted().toList();
        assertTrue(segments.size() >= 2, segments.toString());
        // The oldest segment that is left: with it, everything after it is set aside.
        byte[] one = victim.memory.peek(segments.getFirst(), 120, 1);
        one[0] ^= 0x04;
        victim.memory.overwrite(segments.getFirst(), 120, one);
        victim.start();
        assertTrue(victim.node.view().logRecovering());
        assertEquals(epoch + 1, victim.node.view().logEpoch());
        cluster.ticks(120);
        assertFalse(victim.node.view().logRecovering(), "the log is whole again");
        cluster.write(10, 100, random);
        cluster.assertSameState();
        cluster.close();
    }
}
