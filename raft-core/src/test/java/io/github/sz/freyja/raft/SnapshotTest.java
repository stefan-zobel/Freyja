package io.github.sz.freyja.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.raft.RaftEvent.ClientPropose;
import io.github.sz.freyja.raft.RaftEvent.LogDamaged;
import io.github.sz.freyja.raft.RaftEvent.SnapshotCreated;
import io.github.sz.freyja.raft.RaftEvent.SnapshotInstalled;
import io.github.sz.freyja.raft.RaftEvent.SnapshotOffered;
import io.github.sz.freyja.raft.RaftEvent.SnapshotSendFinished;
import io.github.sz.freyja.raft.RaftMessage.AppendEntries;
import io.github.sz.freyja.raft.RaftMessage.AppendResponse;
import io.github.sz.freyja.raft.RaftMessage.Heartbeat;

/** Log compaction and snapshots (docs/spec/raft.md, section 3, rule 8). */
class SnapshotTest {

    private static final ClusterConfig THREE = ClusterConfig.ofVoters(0, 1, 2);

    private static LogEntry entry(long index, long term) {
        return new LogEntry(index, term, EntryType.COMMAND, new byte[] {(byte) index});
    }

    private static List<LogEntry> entries(long from, long to, long term) {
        List<LogEntry> list = new ArrayList<>();
        for (long i = from; i <= to; i++) {
            list.add(entry(i, term));
        }
        return list;
    }

    /** A leader with a follower that was away from the start and a log cut at index 4. */
    private static TestCluster clusterWithACutLog() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        int id = leader.config.nodeId();
        cluster.isolated.add((id + 1) % 3);
        for (int i = 0; i < 5; i++) {
            leader.step(new ClientPropose(i, new byte[] {(byte) i}));
        }
        cluster.deliverAll();
        cluster.tickAll();
        assertEquals(6, leader.applied.size());

        SnapshotMeta meta = leader.raft.snapshotMeta(4);
        assertEquals(new SnapshotMeta(4, leader.view().term(), THREE), meta);
        leader.step(new SnapshotCreated(meta));
        assertEquals(List.of(4L), leader.prefixTruncations);
        leader.log.startAfter(4, true);
        leader.snapshot = meta;
        return cluster;
    }

    @Test
    void aFollowerThatNeedsEntriesTheLogNoLongerHoldsGetsTheSnapshot() {
        TestCluster cluster = clusterWithACutLog();
        TestNode leader = cluster.leader();
        int id = leader.config.nodeId();
        TestNode behind = cluster.node((id + 1) % 3);
        SnapshotMeta meta = leader.snapshot;

        cluster.isolated.clear();
        for (int i = 0; i < 25 && leader.directives.isEmpty(); i++) {
            cluster.tickAll();
        }
        assertEquals(List.of(new SnapshotDirective.Send(behind.config.nodeId(), leader.view().term(), meta)),
                leader.directives);
        leader.outbox.clear();
        cluster.tickAll();
        assertTrue(leader.outbox.stream().noneMatch(
                o -> o.toNode() == behind.config.nodeId() && o.message() instanceof AppendEntries),
                "no appends while the snapshot is under way");

        // The runtime of the follower has the snapshot staged and asks the core.
        behind.step(new SnapshotOffered(id, leader.view().term(), meta));
        assertEquals(List.of(new SnapshotDirective.Install(id, meta, false, true)), behind.directives);
        behind.log.startAfter(4, false);
        behind.appliedIndex = 4;
        behind.snapshot = meta;
        behind.step(new SnapshotInstalled(meta));
        assertEquals(4, behind.view().lastLogIndex());
        assertEquals(4, behind.view().commitIndex());
        assertEquals(new AppendResponse(leader.view().term(), true, 4, 0, 0, 0), behind.outbox.getLast().message());

        leader.step(new SnapshotSendFinished(behind.config.nodeId(), true));
        for (int i = 0; i < 5; i++) {
            cluster.tickAll();
        }
        assertEquals(leader.view().lastLogIndex(), behind.view().lastLogIndex());
        assertEquals(leader.view().commitIndex(), behind.view().commitIndex());
        assertEquals(6, behind.appliedIndex);
        assertEquals(3, behind.applied.size(),
                "applied one by one: the first entry before it was cut off, and the two after the snapshot");

        // After a restart the node starts from its snapshot.
        behind.restart();
        assertEquals(6, behind.view().lastLogIndex());
        assertEquals(leader.view().term(), behind.view().lastLogTerm());
        assertEquals(THREE, behind.view().config());
    }

    @Test
    void aFailedTransferIsTriedAgain() {
        TestCluster cluster = clusterWithACutLog();
        TestNode leader = cluster.leader();
        int behind = (leader.config.nodeId() + 1) % 3;
        cluster.isolated.clear();
        for (int i = 0; i < 25 && leader.directives.isEmpty(); i++) {
            cluster.tickAll();
        }
        assertEquals(1, leader.directives.size());
        leader.step(new SnapshotSendFinished(behind, false));
        for (int i = 0; i < 25 && leader.directives.size() < 2; i++) {
            cluster.tickAll();
        }
        assertEquals(2, leader.directives.size(), "the follower still needs the snapshot");
    }

    @Test
    void aSnapshotIsRejectedWhenItIsStaleAndNothingMovesWhileOneIsInstalled() {
        TestNode node = new TestNode(RaftConfig.defaults(1, 7), THREE);
        node.autoPersist = true;
        node.receive(0, new AppendEntries(2, 0, 0, entries(1, 5, 2), 3, 0));
        assertEquals(3, node.applied.size());

        SnapshotMeta old = new SnapshotMeta(3, 2, THREE);
        node.step(new SnapshotOffered(0, 2, old));
        assertEquals(new SnapshotDirective.Reject(0, old), node.directives.getLast(), "the log holds all of it");
        SnapshotMeta ofOldLeader = new SnapshotMeta(9, 1, THREE);
        node.step(new SnapshotOffered(2, 1, ofOldLeader));
        assertEquals(new SnapshotDirective.Reject(2, ofOldLeader), node.directives.getLast());

        // The log agrees with the snapshot at its end: the entries after it stay.
        SnapshotMeta fits = new SnapshotMeta(4, 2, THREE);
        node.step(new SnapshotOffered(0, 2, fits));
        assertEquals(new SnapshotDirective.Install(0, fits, true, true), node.directives.getLast());

        // Until it is installed: no second one, no entries, and the commit index stays.
        SnapshotMeta second = new SnapshotMeta(5, 2, THREE);
        node.step(new SnapshotOffered(0, 2, second));
        assertEquals(new SnapshotDirective.Reject(0, second), node.directives.getLast());
        node.outbox.clear();
        node.receive(0, new AppendEntries(2, 5, 2, List.of(entry(6, 2)), 5, 0));
        assertEquals(5, node.view().lastLogIndex());
        assertTrue(node.outbox.isEmpty());
        node.receive(0, new Heartbeat(2, 5, 1));
        assertEquals(3, node.view().commitIndex());
        assertThrows(IllegalArgumentException.class, () -> node.raft.step(new SnapshotInstalled(second)));

        node.log.startAfter(4, true);
        node.appliedIndex = 4;
        node.step(new SnapshotInstalled(fits));
        assertEquals(5, node.view().lastLogIndex());
        assertEquals(4, node.view().commitIndex());
        node.receive(0, new Heartbeat(2, 5, 2));
        assertEquals(5, node.appliedIndex);
    }

    @Test
    void aSnapshotThatDisagreesWithTheLogReplacesIt() {
        TestNode node = new TestNode(RaftConfig.defaults(1, 7), THREE);
        node.autoPersist = true;
        node.receive(0, new AppendEntries(1, 0, 0, entries(1, 5, 1), 0, 0));
        SnapshotMeta meta = new SnapshotMeta(4, 2, THREE);
        node.step(new SnapshotOffered(2, 2, meta));
        assertEquals(new SnapshotDirective.Install(2, meta, false, true), node.directives.getLast());
        node.log.startAfter(4, false);
        node.appliedIndex = 4;
        node.step(new SnapshotInstalled(meta));
        assertEquals(4, node.view().lastLogIndex());
        assertEquals(2, node.view().lastLogTerm());
        node.receive(2, new AppendEntries(2, 4, 2, List.of(entry(5, 2)), 5, 0));
        assertEquals(5, node.appliedIndex);
    }

    @Test
    void aNodeWhoseStateMachineIsAheadOfItsDamagedLogTakesTheSnapshotForItsLogOnly() {
        TestNode node = new TestNode(RaftConfig.defaults(1, 7), THREE);
        node.autoPersist = true;
        node.receive(0, new AppendEntries(1, 0, 0, entries(1, 6, 1), 6, 0));
        assertEquals(6, node.appliedIndex);
        node.step(new LogDamaged(2));
        assertEquals(1, node.view().lastLogIndex());

        // The leader has cut its log at 4: it can only offer the snapshot.
        SnapshotMeta meta = new SnapshotMeta(4, 1, THREE);
        node.step(new SnapshotOffered(0, 1, meta));
        assertEquals(new SnapshotDirective.Install(0, meta, false, false), node.directives.getLast(),
                "the state machine is past the snapshot and must not go back");
        node.log.startAfter(4, false);
        node.step(new SnapshotInstalled(meta));
        assertEquals(4, node.view().lastLogIndex());
        assertTrue(node.view().logRecovering());

        node.receive(0, new AppendEntries(1, 4, 1, entries(5, 7, 1), 7, 1));
        assertEquals(7, node.appliedIndex, "the entry it had not applied yet, and no other again");
        assertEquals(7, node.applied.size());
    }

    @Test
    void theMetaDataOfASnapshotComesFromTheLog() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        cluster.tickAll();
        leader.step(new RaftEvent.ProposeConfigChange(1, new ConfigChange.RemoveVoter((leader.config.nodeId() + 1) % 3)));
        cluster.deliverAll();
        cluster.tickAll();
        long term = leader.view().term();
        assertEquals(new SnapshotMeta(1, term, THREE), leader.raft.snapshotMeta(1));
        assertEquals(new SnapshotMeta(2, term, leader.view().config()), leader.raft.snapshotMeta(2));
        assertThrows(IllegalArgumentException.class, () -> leader.raft.snapshotMeta(3), "not applied");
        assertThrows(IllegalArgumentException.class,
                () -> leader.raft.step(new SnapshotCreated(new SnapshotMeta(2, term, THREE))),
                "the configuration does not fit the log");

        leader.step(new SnapshotCreated(leader.raft.snapshotMeta(2)));
        assertEquals(List.of(2L), leader.prefixTruncations);
        leader.step(new SnapshotCreated(new SnapshotMeta(1, term, THREE)));
        assertEquals(List.of(2L), leader.prefixTruncations, "an older snapshot changes nothing");
        assertEquals(leader.view().config(), leader.raft.snapshotMeta(2).config());
    }
}
