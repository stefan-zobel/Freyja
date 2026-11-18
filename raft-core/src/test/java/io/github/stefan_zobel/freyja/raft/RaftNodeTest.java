package io.github.stefan_zobel.freyja.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.raft.Notification.ProposalAccepted;
import io.github.stefan_zobel.freyja.raft.Notification.ProposalRejected;
import io.github.stefan_zobel.freyja.raft.Notification.Reason;
import io.github.stefan_zobel.freyja.raft.RaftEvent.ClientPropose;
import io.github.stefan_zobel.freyja.raft.RaftEvent.Persisted;
import io.github.stefan_zobel.freyja.raft.RaftMessage.AppendEntries;
import io.github.stefan_zobel.freyja.raft.RaftMessage.AppendResponse;
import io.github.stefan_zobel.freyja.raft.RaftMessage.RequestVote;
import io.github.stefan_zobel.freyja.raft.RaftMessage.VoteResponse;

/** The protocol rules of the core, driven by hand (docs/spec/raft.md). */
class RaftNodeTest {

    private static final ClusterConfig THREE = ClusterConfig.ofVoters(0, 1, 2);

    private static TestNode follower(int id) {
        return new TestNode(RaftConfig.defaults(id, 7), THREE);
    }

    private static LogEntry entry(long index, long term) {
        return new LogEntry(index, term, EntryType.COMMAND, new byte[] {(byte) index, (byte) term});
    }

    private static void propose(TestNode node, long requestId, int size) {
        node.step(new ClientPropose(requestId, new byte[size]));
    }

    private static List<String> shape(List<LogEntry> entries) {
        List<String> result = new ArrayList<>();
        for (LogEntry entry : entries) {
            result.add(entry.index() + "@" + entry.term() + ":" + entry.type());
        }
        return result;
    }

    // ------------------------------------------------------------------ elections

    @Test
    void aSingleNodeBecomesLeaderOnlyAfterItsOwnVoteIsDurable() {
        TestNode node = new TestNode(RaftConfig.defaults(0, 1), ClusterConfig.ofVoters(0));
        while (node.view().role() == Role.FOLLOWER) {
            node.tick(1);
        }
        assertEquals(Role.CANDIDATE, node.view().role());
        assertEquals(1, node.view().term());
        assertEquals(new HardState(1, 0, false), node.pending.getFirst().hardState());

        node.persistNext();
        assertEquals(Role.LEADER, node.view().role());
        assertEquals(1, node.view().lastLogIndex(), "the entry of the new term");
        assertEquals(0, node.view().commitIndex(), "not durable, so not committed");

        propose(node, 77, 3);
        assertEquals(List.of(new ProposalAccepted(77, 2, 1)), node.notes(ProposalAccepted.class));
        assertTrue(node.applied.isEmpty());
        node.persistAll();
        assertEquals(2, node.view().commitIndex());
        assertEquals(List.of("1@1:NOOP", "2@1:COMMAND"), shape(node.applied));
    }

    @Test
    void threeNodesElectOneLeaderAndReplicate() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        long term = leader.view().term();
        for (int i = 0; i < 5; i++) {
            propose(leader, i, 10);
        }
        cluster.tickAll();
        cluster.tickAll();
        for (TestNode node : cluster.nodes) {
            assertEquals(6, node.view().commitIndex());
            assertEquals(term, node.view().term());
            assertEquals(leader.config.nodeId(), node.view().leaderId());
            assertEquals(shape(leader.applied), shape(node.applied));
            assertEquals(6, node.applied.size());
        }
        assertEquals(5, leader.notes(ProposalAccepted.class).size());
    }

    @Test
    void aGrantedVoteLeavesOnlyAfterItIsDurable() {
        TestNode node = follower(1);
        node.receive(0, new RequestVote(1, 0, 0, false));
        assertTrue(node.outbox.isEmpty(), "the vote is not durable yet");
        assertEquals(new HardState(1, 0, false), node.pending.getFirst().hardState());

        // The same term, another candidate: the rejection promises nothing and leaves at once.
        node.receive(2, new RequestVote(1, 0, 0, false));
        assertEquals(List.of(new Outbound(2, new VoteResponse(1, false, false))), node.outbox);
        node.outbox.clear();

        node.persistNext();
        assertEquals(List.of(new Outbound(0, new VoteResponse(1, false, true))), node.outbox);
    }

    @Test
    void aVoteIsNotGrantedToAStaleLog() {
        TestNode node = follower(1);
        node.autoPersist = true;
        node.receive(0, new AppendEntries(2, 0, 0, List.of(entry(1, 2), entry(2, 2)), 0));
        node.outbox.clear();
        node.tick(10); // the lease of the leader runs out

        node.receive(2, new RequestVote(3, 5, 1, false)); // longer, but an older last term
        assertEquals(new VoteResponse(3, false, false), node.outbox.getLast().message());
        node.receive(2, new RequestVote(3, 1, 2, false)); // same last term, shorter
        assertEquals(new VoteResponse(3, false, false), node.outbox.getLast().message());
        node.receive(2, new RequestVote(3, 2, 2, false));
        assertEquals(new VoteResponse(3, false, true), node.outbox.getLast().message());
    }

    @Test
    void aPreVoteChangesNothingAndAnIsolatedNodeKeepsItsTerm() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        long term = leader.view().term();
        TestNode lonely = cluster.node((leader.config.nodeId() + 1) % 3);

        cluster.isolated.add(lonely.config.nodeId());
        for (int i = 0; i < 200; i++) {
            cluster.tickAll();
        }
        assertEquals(term, lonely.view().term(), "pre-votes do not raise the term");
        assertEquals(Role.PRE_CANDIDATE, lonely.view().role());
        assertNull(lonely.readies.getLast().hardState());

        cluster.isolated.clear();
        for (int i = 0; i < 30; i++) {
            cluster.tickAll();
        }
        assertEquals(leader, cluster.leader(), "the returning node does not disturb the leader");
        assertEquals(term, leader.view().term());
        assertEquals(Role.FOLLOWER, lonely.view().role());

        // A pre-vote request itself changes no state on the receiver.
        TestNode node = follower(1);
        node.receive(0, new RequestVote(5, 0, 0, true));
        assertEquals(List.of(new Outbound(0, new VoteResponse(5, true, true))), node.outbox);
        assertEquals(0, node.view().term());
        assertTrue(node.pending.isEmpty());
    }

    @Test
    void aNodeInContactWithALeaderIgnoresVoteRequests() {
        TestNode node = follower(1);
        node.autoPersist = true;
        node.receive(0, new AppendEntries(1, 0, 0, List.of(), 0));
        node.outbox.clear();

        node.tick(5);
        node.receive(2, new RequestVote(9, 100, 9, false));
        node.receive(2, new RequestVote(9, 100, 9, true));
        assertTrue(node.outbox.isEmpty());
        assertEquals(1, node.view().term());

        node.tick(5); // the minimum election timeout has passed without the leader
        node.receive(2, new RequestVote(9, 100, 9, false));
        assertEquals(9, node.view().term());
        assertEquals(List.of(new Outbound(2, new VoteResponse(9, false, true))), node.outbox);
    }

    @Test
    void aLeaderStepsDownOnRequestAndKeepsWhatItHasStored() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        long term = leader.view().term();
        propose(leader, 1, 4);
        cluster.tickAll();
        long last = leader.view().lastLogIndex();

        for (TestNode node : cluster.nodes) {
            if (node != leader) {
                node.step(new RaftEvent.StepDown());
                assertEquals(Role.FOLLOWER, node.view().role(), "a follower ignores the request");
                assertEquals(leader.config.nodeId(), node.view().leaderId());
            }
        }
        leader.step(new RaftEvent.StepDown());
        assertEquals(Role.FOLLOWER, leader.view().role());
        assertEquals(term, leader.view().term(), "the term does not change");
        assertEquals(last, leader.view().lastLogIndex());
        propose(leader, 2, 4);
        assertEquals(Reason.NOT_LEADER, leader.notes(ProposalRejected.class).getLast().reason());

        TestNode next = cluster.electLeader();
        assertTrue(next.view().term() > term);
        propose(next, 3, 4);
        cluster.tickAll();
        assertEquals(next.view().commitIndex(), leader.view().commitIndex());
    }

    @Test
    void aLeaderWithoutAQuorumStepsDownAndFollowsItsSuccessor() {
        TestCluster cluster = new TestCluster(3);
        TestNode old = cluster.electLeader();
        long term = old.view().term();
        cluster.isolated.add(old.config.nodeId());
        propose(old, 1, 4); // never committed

        old.tick(25);
        assertEquals(Role.FOLLOWER, old.view().role(), "no majority was heard from");
        propose(old, 2, 4);
        assertEquals(Reason.NOT_LEADER, old.notes(ProposalRejected.class).getLast().reason());

        TestNode successor = cluster.electLeader();
        assertNotEquals(old, successor);
        assertTrue(successor.view().term() > term);
        propose(successor, 3, 4);
        cluster.tickAll();

        cluster.isolated.clear();
        for (int i = 0; i < 30; i++) {
            cluster.tickAll();
        }
        assertEquals(successor, cluster.leader());
        for (TestNode node : cluster.nodes) {
            assertEquals(shape(successor.log.entries), shape(node.log.entries));
            assertEquals(shape(successor.applied), shape(node.applied));
        }
        assertEquals(3, successor.applied.size(), "two entries of new terms and one command");
    }

    // ------------------------------------------------------------------ durability

    @Test
    void anAcknowledgmentLeavesOnlyAfterTheEntriesAreDurable() {
        TestNode node = follower(1);
        node.receive(0, new AppendEntries(1, 0, 0, List.of(entry(1, 1), entry(2, 1)), 0));
        assertTrue(node.outbox.isEmpty());
        Ready write = node.pending.getFirst();
        assertEquals(new HardState(1, HardState.NO_VOTE, false), write.hardState());
        assertEquals(2, write.entries().size());
        assertEquals(0, node.view().durableIndex());

        node.persistNext();
        assertEquals(List.of(new Outbound(0, new AppendResponse(1, true, 2, 0, 0))), node.outbox);
        assertEquals(2, node.view().durableIndex());
        node.outbox.clear();

        // Committed but not durable entries are not applied.
        node.receive(0, new AppendEntries(1, 2, 1, List.of(entry(3, 1)), 3));
        assertEquals(3, node.view().commitIndex());
        assertEquals(2, node.applied.size());
        node.persistNext();
        assertEquals(3, node.applied.size());
        assertEquals(new AppendResponse(1, true, 3, 0, 0), node.outbox.getLast().message());
    }

    @Test
    void aRejectionAndAHeartbeatAnswerLeaveAtOnce() {
        TestNode node = follower(1);
        node.receive(0, new AppendEntries(1, 4, 1, List.of(entry(5, 1)), 0));
        assertEquals(List.of(new Outbound(0, new AppendResponse(1, false, 4, 0, 0))), node.outbox);
        node.outbox.clear();
        node.receive(0, new RaftMessage.Heartbeat(1, 0, 3));
        assertEquals(List.of(new Outbound(0, new RaftMessage.HeartbeatResponse(1, 3))), node.outbox);
        assertEquals(1, node.pending.size(), "the new term is still to be persisted");
    }

    @Test
    void theLeaderCountsItselfOnlyUpToItsDurableIndex() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        cluster.tickAll();
        int id = leader.config.nodeId();
        TestNode absent = cluster.node((id + 1) % 3);
        long commit = leader.view().commitIndex();

        leader.autoPersist = false;
        cluster.isolated.add(absent.config.nodeId());
        propose(leader, 1, 4);
        cluster.deliverAll();
        assertEquals(commit, leader.view().commitIndex(), "one follower alone is no majority");
        leader.persistAll();
        assertEquals(commit + 1, leader.view().commitIndex());
        assertEquals(commit + 1, leader.applied.size());

        // With both followers the entry commits without the leader, but it is not applied
        // before it is durable on the leader as well.
        cluster.isolated.clear();
        cluster.tickAll();
        propose(leader, 2, 4);
        cluster.deliverAll();
        assertEquals(commit + 2, leader.view().commitIndex());
        assertEquals(commit + 1, leader.applied.size());
        leader.persistAll();
        assertEquals(commit + 2, leader.applied.size());
    }

    @Test
    void aConflictingSuffixIsTruncatedInMemoryAndInTheStore() {
        TestNode node = follower(1);
        node.receive(0, new AppendEntries(1, 0, 0, List.of(entry(1, 1), entry(2, 1), entry(3, 1)), 0));
        node.persistAll();
        node.outbox.clear();

        node.receive(2, new AppendEntries(2, 1, 1, List.of(entry(2, 2)), 0));
        Ready write = node.pending.getFirst();
        assertEquals(2, write.truncateSuffixFrom());
        assertEquals(List.of("2@2:COMMAND"), shape(write.entries()));
        assertEquals(2, node.view().lastLogIndex());
        assertEquals(2, node.view().lastLogTerm());
        assertEquals(1, node.view().durableIndex(), "the stored suffix no longer counts");
        assertTrue(node.outbox.isEmpty());

        node.persistAll();
        assertEquals(List.of("1@1:COMMAND", "2@2:COMMAND"), shape(node.log.entries));
        assertEquals(List.of(new Outbound(2, new AppendResponse(2, true, 2, 0, 0))), node.outbox);
    }

    @Test
    void aWriteOnItsWayDoesNotBringBackTruncatedEntries() {
        TestNode node = follower(1);
        node.receive(0, new AppendEntries(1, 0, 0, List.of(entry(1, 1), entry(2, 1)), 0));
        assertEquals(1, node.pending.size());
        // Before that write is durable, a leader of a later term replaces both entries.
        node.receive(2, new AppendEntries(2, 0, 0, List.of(entry(1, 2)), 0));
        assertEquals(2, node.pending.size());
        Ready second = node.pending.get(1);
        assertEquals(1, second.truncateSuffixFrom());
        assertEquals(List.of("1@2:COMMAND"), shape(second.entries()));

        node.persistNext();
        assertEquals(0, node.view().durableIndex(), "the first write made nothing of the log durable");
        assertEquals(1, node.view().lastLogIndex());
        assertEquals(2, node.view().lastLogTerm());
        node.persistNext();
        assertEquals(1, node.view().durableIndex());
        assertEquals(List.of("1@2:COMMAND"), shape(node.log.entries));
    }

    @Test
    void entriesNeverHandedOutAreTruncatedWithoutTouchingTheStore() {
        TestNode node = follower(1);
        node.raft.step(new RaftEvent.MessageReceived(0,
                new AppendEntries(1, 0, 0, List.of(entry(1, 1), entry(2, 1)), 0)));
        node.raft.step(new RaftEvent.MessageReceived(2,
                new AppendEntries(2, 1, 1, List.of(entry(2, 2), entry(3, 2)), 0)));
        Ready ready = node.raft.takeReady();
        assertEquals(-1, ready.truncateSuffixFrom());
        assertEquals(List.of("1@1:COMMAND", "2@2:COMMAND", "3@2:COMMAND"), shape(ready.entries()));
        assertEquals(new HardState(2, HardState.NO_VOTE, false), ready.hardState());
    }

    @Test
    void aRestartedNodeKeepsItsVoteAndForgetsWhatWasNotDurable() {
        TestNode node = follower(1);
        node.receive(0, new RequestVote(1, 0, 0, false));
        node.persistAll();
        node.restart();
        assertEquals(1, node.view().term());
        assertEquals(0, node.view().votedFor());
        node.receive(2, new RequestVote(1, 0, 0, false));
        assertEquals(List.of(new Outbound(2, new VoteResponse(1, false, false))), node.outbox);

        TestNode other = follower(1);
        other.receive(0, new RequestVote(1, 0, 0, false));
        assertTrue(other.outbox.isEmpty(), "nothing was promised");
        other.restart();
        assertEquals(0, other.view().term());
        assertEquals(HardState.NO_VOTE, other.view().votedFor());
    }

    @Test
    void aPersistedEventForAnUnknownReadyIsRejected() {
        TestNode node = follower(1);
        assertThrows(IllegalArgumentException.class, () -> node.raft.step(new Persisted(1)));
        node.receive(0, new RequestVote(1, 0, 0, false));
        node.persistAll();
        node.step(new Persisted(1)); // a repeated report is harmless
        assertEquals(1, node.outbox.size());
    }

    @Test
    void anEntryOfAnEarlierTermIsCommittedOnlyThroughAnEntryOfTheCurrentTerm() {
        TestNode node = follower(0);
        node.autoPersist = true;
        node.receive(1, new AppendEntries(2, 0, 0, List.of(entry(1, 2)), 0));
        while (node.view().role() != Role.PRE_CANDIDATE) {
            node.tick(1);
        }
        node.receive(1, new VoteResponse(3, true, true));
        assertEquals(Role.CANDIDATE, node.view().role());
        node.receive(1, new VoteResponse(3, false, true));
        assertEquals(Role.LEADER, node.view().role());
        assertEquals(2, node.view().lastLogIndex());
        assertEquals(2, node.view().durableIndex());

        // Node 2 lacks everything; the leader finds that out and sends both entries.
        node.receive(2, new AppendResponse(3, false, 1, 0, 0));
        // Node 2 has stored the entry of term 2: it is on a majority now, but it may still be
        // replaced by a leader of a later term that does not have it. It is not committed.
        node.receive(2, new AppendResponse(3, true, 1, 0, 0));
        assertEquals(0, node.view().commitIndex());
        assertTrue(node.applied.isEmpty());
        // With the entry of term 3 on a majority, both are committed.
        node.receive(2, new AppendResponse(3, true, 2, 0, 0));
        assertEquals(2, node.view().commitIndex());
        assertEquals(List.of("1@2:COMMAND", "2@3:NOOP"), shape(node.applied));
    }

    // ------------------------------------------------------------------ flow control

    @Test
    void theInflightWindowAndTheMessageSizeLimitAppends() {
        RaftConfig template = RaftConfig.defaults(0, 0).withFlowControl(2, 10, 1 << 20);
        TestCluster cluster = new TestCluster(3, template);
        TestNode leader = cluster.electLeader();
        cluster.tickAll();
        int follower = (leader.config.nodeId() + 1) % 3;

        for (int i = 0; i < 10; i++) {
            propose(leader, i, 6);
        }
        int messages = 0;
        for (Outbound outbound : leader.outbox) {
            if (outbound.toNode() == follower && outbound.message() instanceof AppendEntries append) {
                messages++;
                assertEquals(1, append.entries().size(), "two entries of 6 bytes exceed 10 bytes");
            }
        }
        assertEquals(2, messages, "the window holds two unacknowledged appends");

        cluster.deliverAll();
        cluster.tickAll();
        for (TestNode node : cluster.nodes) {
            assertEquals(11, node.applied.size());
        }
    }

    @Test
    void proposalsAreRejectedWhenTheUncommittedLogIsTooLarge() {
        RaftConfig template = RaftConfig.defaults(0, 0).withFlowControl(256, 1 << 20, 25);
        TestCluster cluster = new TestCluster(3, template);
        TestNode leader = cluster.electLeader();
        cluster.tickAll();
        int id = leader.config.nodeId();

        propose(leader, 1, 10);
        propose(leader, 2, 10);
        propose(leader, 3, 10);
        assertEquals(2, leader.notes(ProposalAccepted.class).size());
        assertEquals(List.of(new ProposalRejected(3, Reason.OVERLOADED, id)),
                leader.notes(ProposalRejected.class));

        cluster.deliverAll(); // commits and frees the backlog
        propose(leader, 4, 10);
        assertEquals(3, leader.notes(ProposalAccepted.class).size());

        // One proposal is always accepted when nothing is uncommitted, whatever its size.
        cluster.deliverAll();
        propose(leader, 5, 1000);
        assertEquals(4, leader.notes(ProposalAccepted.class).size());

        TestNode other = cluster.node((id + 1) % 3);
        propose(other, 6, 1);
        assertEquals(List.of(new ProposalRejected(6, Reason.NOT_LEADER, id)),
                other.notes(ProposalRejected.class));
    }

    // ------------------------------------------------------------------ recovery of followers

    /** A leader is cut off with uncommitted entries while the others move on. */
    private static String divergeAndHeal(List<TestNode> out) {
        TestCluster cluster = new TestCluster(3);
        TestNode old = cluster.electLeader();
        cluster.tickAll();
        cluster.isolated.add(old.config.nodeId());
        for (int i = 0; i < 5; i++) {
            propose(old, i, 3);
        }
        TestNode successor = cluster.electLeader();
        for (int i = 10; i < 13; i++) {
            propose(successor, i, 3);
        }
        cluster.tickAll();
        cluster.isolated.clear();
        for (int i = 0; i < 40; i++) {
            cluster.tickAll();
        }
        out.addAll(cluster.nodes);
        StringBuilder all = new StringBuilder();
        for (TestNode node : cluster.nodes) {
            all.append(node.readies).append('\n');
        }
        return all.toString();
    }

    @Test
    void aDivergedLogIsRepairedByTheNewLeader() {
        List<TestNode> nodes = new ArrayList<>();
        divergeAndHeal(nodes);
        TestNode first = nodes.getFirst();
        assertEquals(5, first.applied.size(), "entries of two terms and three commands");
        assertNotNull(first.log.entries.getLast());
        boolean truncated = false;
        for (TestNode node : nodes) {
            assertEquals(shape(first.log.entries), shape(node.log.entries));
            assertEquals(shape(first.applied), shape(node.applied));
            assertEquals(5, node.view().commitIndex());
            truncated |= node.readies.stream().anyMatch(r -> r.truncateSuffixFrom() != -1);
        }
        assertTrue(truncated, "the old leader had to drop its uncommitted entries");
        assertFalse(first.applied.stream().anyMatch(e -> e.type() == EntryType.CONFIG));
    }

    @Test
    void theSameEventsProduceTheSameWork() {
        String first = divergeAndHeal(new ArrayList<>());
        String second = divergeAndHeal(new ArrayList<>());
        assertEquals(first, second);
        assertTrue(first.length() > 1000);
    }
}
