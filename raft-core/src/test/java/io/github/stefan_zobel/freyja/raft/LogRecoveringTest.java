package io.github.stefan_zobel.freyja.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.raft.RaftEvent.ClientPropose;
import io.github.stefan_zobel.freyja.raft.RaftEvent.LogDamaged;
import io.github.stefan_zobel.freyja.raft.RaftMessage.AppendEntries;
import io.github.stefan_zobel.freyja.raft.RaftMessage.AppendResponse;
import io.github.stefan_zobel.freyja.raft.RaftMessage.RequestVote;
import io.github.stefan_zobel.freyja.raft.RaftMessage.VoteResponse;

/** Log-recovering mode (docs/spec/raft.md, section 8). */
class LogRecoveringTest {

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

    /** A follower of node 0 in term 1 with five durable entries, four of them applied. */
    private static TestNode followerWithFiveEntries() {
        TestNode node = new TestNode(RaftConfig.defaults(1, 7), THREE);
        node.autoPersist = true;
        node.receive(0, new AppendEntries(1, 0, 0, entries(1, 5, 1), 4, 0));
        assertEquals(4, node.applied.size());
        node.outbox.clear();
        return node;
    }

    @Test
    void aDamagedLogIsCutOffAndTheNodeVotesAsWithTheLogItHasLost() {
        TestNode node = followerWithFiveEntries();
        node.autoPersist = false;
        node.step(new LogDamaged(3));

        Ready write = node.pending.getFirst();
        assertEquals(new HardState(1, HardState.NO_VOTE, true, 1, 5, 1), write.hardState(),
                "the flag, the new epoch, and the end of the lost log are persisted with the truncation");
        assertEquals(3, write.truncateSuffixFrom());
        assertTrue(node.view().logRecovering());
        assertEquals(2, node.view().lastLogIndex());
        assertEquals(2, node.view().commitIndex(), "committed entries are gone from this log");
        assertEquals(4, node.applied.size(), "what was applied stays applied");
        node.persistAll();

        // A candidate that is ahead of the log this node holds now, but behind the log it has
        // lost, gets no vote and no pre-vote: it may lack entries this node has acknowledged.
        node.tick(10);
        node.receive(2, new RequestVote(2, 4, 1, false));
        assertEquals(new VoteResponse(2, false, false), node.outbox.getLast().message());
        node.receive(2, new RequestVote(3, 4, 1, true));
        assertEquals(new VoteResponse(2, true, false), node.outbox.getLast().message());
        // No election of its own.
        node.tick(200);
        assertEquals(Role.FOLLOWER, node.view().role());
        assertEquals(2, node.view().term());
        // A candidate that covers the lost log gets the vote, as it would have before.
        node.receive(2, new RequestVote(3, 5, 1, true));
        assertEquals(new VoteResponse(3, true, true), node.outbox.getLast().message());
        node.receive(2, new RequestVote(3, 5, 1, false));
        node.persistAll();
        assertEquals(new VoteResponse(3, false, true), node.outbox.getLast().message());

        assertThrows(IllegalArgumentException.class, () -> node.raft.step(new LogDamaged(0)));
    }

    @Test
    void damageBeyondTheLogTheNodeWasStartedWithLeavesItWithoutAVote() {
        // The runtime found the damage when it opened the log: the node never saw the lost
        // entries and cannot know how far they went.
        TestNode node = followerWithFiveEntries();
        node.step(new LogDamaged(6));
        assertTrue(node.view().logRecovering());
        assertEquals(5, node.view().lastLogIndex(), "the log it holds is not affected");
        assertEquals(new HardState(1, HardState.NO_VOTE, true, 1, 5, HardState.UNKNOWN_TERM), node.hardState);
        node.tick(10);
        node.receive(2, new RequestVote(2, 1000, 1, false));
        assertEquals(new VoteResponse(2, false, false), node.outbox.getLast().message());
        node.tick(200);
        assertEquals(Role.FOLLOWER, node.view().role());
    }

    @Test
    void theNodeLeavesTheModeOnlyOnAnAppendThatKnowsItsEpochAndCoversTheCommitIndex() {
        TestNode node = followerWithFiveEntries();
        node.step(new LogDamaged(3));
        node.outbox.clear();

        // The leader does not know yet: its append does not fit, and the answer names the epoch.
        node.receive(0, new AppendEntries(1, 5, 1, List.of(entry(6, 1)), 5, 0));
        assertEquals(new AppendResponse(1, false, 5, 2, 1, 1), node.outbox.getLast().message());

        // An old append that fits, sent before the leader knew: the log grows, the mode stays.
        node.receive(0, new AppendEntries(1, 2, 1, List.of(entry(3, 1)), 3, 0));
        assertEquals(3, node.view().lastLogIndex());
        assertTrue(node.view().logRecovering());

        // The leader knows the epoch, but this log does not reach its commit index yet.
        node.receive(0, new AppendEntries(1, 3, 1, List.of(entry(4, 1)), 5, 1));
        assertTrue(node.view().logRecovering());
        assertEquals(4, node.applied.size());

        node.receive(0, new AppendEntries(1, 4, 1, List.of(entry(5, 1), entry(6, 1)), 5, 1));
        assertFalse(node.view().logRecovering());
        assertEquals(new HardState(1, HardState.NO_VOTE, false, 1), node.hardState);
        assertEquals(5, node.applied.size(), "nothing was applied twice");
        assertEquals(new AppendResponse(1, true, 6, 0, 0, 1), node.outbox.getLast().message());

        // It votes again.
        node.tick(10);
        node.receive(2, new RequestVote(2, 6, 1, false));
        assertEquals(new VoteResponse(2, false, true), node.outbox.getLast().message());
    }

    @Test
    void theCommitIndexOfALeaderOfAnotherTermDoesNotEndTheMode() {
        TestNode node = followerWithFiveEntries();
        node.step(new LogDamaged(3));
        // A new leader that has not committed an entry of its own term: entries of earlier
        // leaders may be committed without it knowing.
        node.receive(2, new AppendEntries(2, 2, 1, entries(3, 5, 1), 5, 1));
        assertEquals(5, node.view().lastLogIndex());
        assertTrue(node.view().logRecovering());
        node.receive(2, new AppendEntries(2, 5, 1, List.of(entry(6, 2)), 6, 1));
        assertFalse(node.view().logRecovering());
    }

    @Test
    void anAcknowledgmentThatWaitsForLostEntriesIsNeverSent() {
        TestNode node = new TestNode(RaftConfig.defaults(1, 7), THREE);
        node.receive(0, new AppendEntries(1, 0, 0, entries(1, 3, 1), 0, 0));
        assertTrue(node.outbox.isEmpty(), "waits for the entries to become durable");
        node.step(new LogDamaged(2));
        node.persistAll();
        assertTrue(node.outbox.stream().noneMatch(
                o -> o.message() instanceof AppendResponse response && response.success()), node.outbox.toString());
    }

    @Test
    void aLeaderForgetsWhatAFollowerOfANewEpochHadAndRepairsIt() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        for (int i = 0; i < 5; i++) {
            leader.step(new ClientPropose(i, new byte[] {(byte) i}));
        }
        for (int i = 0; i < 5; i++) {
            cluster.tickAll();
        }
        int id = leader.config.nodeId();
        TestNode damaged = cluster.node((id + 1) % 3);
        TestNode third = cluster.node((id + 2) % 3);
        assertEquals(6, damaged.applied.size());

        damaged.step(new LogDamaged(2));
        assertEquals(1, damaged.view().lastLogIndex());
        // With the third node away, the leader must not count what the damaged node had.
        cluster.isolated.add(third.config.nodeId());
        leader.step(new ClientPropose(10, new byte[1]));
        cluster.deliverAll();
        for (int i = 0; i < 40; i++) {
            cluster.tickAll();
        }
        assertFalse(damaged.view().logRecovering(), "repaired by the leader");
        assertEquals(leader.view().lastLogIndex(), damaged.view().lastLogIndex());
        assertEquals(leader.view().lastLogIndex(), leader.view().commitIndex(),
                "the repaired node counts again");
        assertEquals(leader.applied.size(), damaged.applied.size());
        for (int i = 0; i < leader.log.entries.size(); i++) {
            assertEquals(leader.log.entries.get(i).term(), damaged.log.entries.get(i).term());
        }
    }

    @Test
    void aLeaderWhoseLogIsDamagedStepsDown() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        for (int i = 0; i < 5; i++) {
            cluster.tickAll();
        }
        leader.step(new LogDamaged(1));
        assertEquals(Role.FOLLOWER, leader.view().role());
        assertTrue(leader.view().logRecovering());
        assertEquals(0, leader.view().lastLogIndex());

        cluster.isolated.clear();
        TestNode successor = null;
        for (int i = 0; i < 200 && successor == null; i++) {
            cluster.tickAll();
            successor = cluster.leader();
        }
        assertTrue(successor != null && successor != leader, "the others elect a leader without it");
        for (int i = 0; i < 40; i++) {
            cluster.tickAll();
        }
        assertFalse(leader.view().logRecovering());
        assertEquals(successor.view().lastLogIndex(), leader.view().lastLogIndex());
    }

    @Test
    void aRestartedNodeMayHaveAppliedMoreThanItsLogHoldsOnlyInTheMode() {
        TestNode node = followerWithFiveEntries();
        node.step(new LogDamaged(3));
        node.restart();
        assertTrue(node.view().logRecovering());
        assertEquals(0, node.view().commitIndex(), "it learns from the leader what is committed");
        assertEquals(4, node.applied.size());

        TestNode.MemoryLog empty = new TestNode.MemoryLog();
        assertThrows(IllegalArgumentException.class, () -> RaftNode.create(RaftConfig.defaults(1, 7),
                PersistedState.initial(THREE), empty, 4));
    }
}
