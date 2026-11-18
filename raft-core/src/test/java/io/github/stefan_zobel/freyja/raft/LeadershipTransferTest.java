package io.github.stefan_zobel.freyja.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.raft.Notification.ProposalAccepted;
import io.github.stefan_zobel.freyja.raft.Notification.ProposalRejected;
import io.github.stefan_zobel.freyja.raft.Notification.Reason;
import io.github.stefan_zobel.freyja.raft.Notification.TransferAborted;
import io.github.stefan_zobel.freyja.raft.RaftEvent.ClientPropose;
import io.github.stefan_zobel.freyja.raft.RaftEvent.ProposeConfigChange;
import io.github.stefan_zobel.freyja.raft.RaftEvent.TransferLeadership;
import io.github.stefan_zobel.freyja.raft.RaftMessage.RequestVote;
import io.github.stefan_zobel.freyja.raft.RaftMessage.TimeoutNow;

/** Leadership transfer (docs/spec/raft.md, section 7). */
class LeadershipTransferTest {

    private static void settle(TestCluster cluster) {
        for (int i = 0; i < 5; i++) {
            cluster.tickAll();
        }
    }

    @Test
    void anUpToDateTargetTakesOverAtOnceWithoutAPreVote() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        settle(cluster);
        int id = leader.config.nodeId();
        TestNode target = cluster.node((id + 1) % 3);
        long term = leader.view().term();

        leader.step(new TransferLeadership(1, target.config.nodeId()));
        assertEquals(List.of(new TimeoutNow(term)), leader.sent(TimeoutNow.class));
        // The others have heard from the leader just now; they vote all the same, because
        // the request says that the leader asked for the election.
        cluster.deliverAll();
        assertEquals(Role.LEADER, target.view().role());
        assertEquals(term + 1, target.view().term(), "one election, no pre-vote round before it");
        assertEquals(Role.FOLLOWER, leader.view().role());
        assertEquals(target.config.nodeId(), leader.view().leaderId());
        assertTrue(leader.notes(TransferAborted.class).isEmpty());

        target.step(new ClientPropose(5, new byte[1]));
        cluster.deliverAll();
        assertEquals(target.view().lastLogIndex(), target.view().commitIndex());
    }

    @Test
    void aRequestOfATransferIsMarkedAndAnOrdinaryOneIsIgnoredWhileALeaderIsHeardFrom() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        settle(cluster);
        int id = leader.config.nodeId();
        TestNode target = cluster.node((id + 1) % 3);
        TestNode other = cluster.node((id + 2) % 3);
        long term = leader.view().term();

        // An ordinary request of a later term: the node is in contact with its leader.
        other.receive(target.config.nodeId(), new RequestVote(term + 1, 100, term, false));
        assertEquals(term, other.view().term());
        assertTrue(other.outbox.isEmpty());

        target.receive(id, new TimeoutNow(term));
        assertEquals(Role.CANDIDATE, target.view().role());
        List<RequestVote> requests = target.sent(RequestVote.class);
        assertFalse(requests.isEmpty());
        assertTrue(requests.stream().allMatch(r -> r.transfer() && !r.preVote() && r.term() == term + 1));

        // A node that does not follow the sender ignores the order.
        TestNode stranger = new TestNode(RaftConfig.defaults(1, 3), ClusterConfig.ofVoters(0, 1, 2));
        stranger.receive(2, new TimeoutNow(0));
        assertEquals(Role.FOLLOWER, stranger.view().role());
    }

    @Test
    void aTargetThatIsBehindIsBroughtUpToDateFirst() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        settle(cluster);
        int id = leader.config.nodeId();
        TestNode target = cluster.node((id + 1) % 3);

        cluster.isolated.add(target.config.nodeId());
        for (int i = 0; i < 3; i++) {
            leader.step(new ClientPropose(i, new byte[1]));
        }
        cluster.deliverAll();
        cluster.isolated.clear();
        leader.outbox.clear();

        leader.step(new TransferLeadership(1, target.config.nodeId()));
        assertTrue(leader.sent(TimeoutNow.class).isEmpty(), "the target lacks entries");
        leader.step(new ClientPropose(9, new byte[1]));
        assertEquals(new ProposalRejected(9, Reason.TRANSFER_IN_PROGRESS, target.config.nodeId()),
                leader.notes(ProposalRejected.class).getLast());
        leader.step(new ProposeConfigChange(10, new ConfigChange.AddLearner(7)));
        assertEquals(Reason.TRANSFER_IN_PROGRESS, leader.notes(ProposalRejected.class).getLast().reason());
        leader.step(new TransferLeadership(2, (id + 2) % 3));
        assertEquals(Reason.TRANSFER_IN_PROGRESS, leader.notes(ProposalRejected.class).getLast().reason());

        for (int i = 0; i < 9 && target.view().role() != Role.LEADER; i++) {
            cluster.tickAll(); // the lost appends are found and sent again, then the order follows
        }
        for (int i = 0; i < 30 && target.view().role() != Role.LEADER; i++) {
            cluster.tickAll();
        }
        // Whether the transfer made it within the election timeout or was aborted and the
        // cluster carried on: nothing was lost, and the leader accepts proposals again.
        TestNode now = cluster.electLeader();
        now.step(new ClientPropose(11, new byte[1]));
        cluster.deliverAll();
        assertEquals(now.view().lastLogIndex(), now.view().commitIndex());
        assertEquals(11, now.notes(ProposalAccepted.class).getLast().requestId());
    }

    @Test
    void aTransferThatDoesNotSucceedIsAbortedAfterOneElectionTimeout() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        settle(cluster);
        int id = leader.config.nodeId();
        int target = (id + 1) % 3;
        long term = leader.view().term();

        cluster.isolated.add(target);
        leader.step(new TransferLeadership(1, target));
        for (int i = 0; i < 9; i++) {
            cluster.tickAll();
        }
        assertTrue(leader.notes(TransferAborted.class).isEmpty());
        leader.step(new ClientPropose(3, new byte[1]));
        assertEquals(Reason.TRANSFER_IN_PROGRESS, leader.notes(ProposalRejected.class).getLast().reason());
        cluster.tickAll();
        assertEquals(List.of(new TransferAborted(1, target)), leader.notes(TransferAborted.class));
        assertEquals(Role.LEADER, leader.view().role());
        assertEquals(term, leader.view().term());
        leader.step(new ClientPropose(4, new byte[1]));
        cluster.deliverAll();
        assertEquals(leader.view().lastLogIndex(), leader.view().commitIndex());
    }

    @Test
    void aTransferToTheLeaderItselfOrToANodeThatIsNoVoterIsRejected() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        settle(cluster);
        int id = leader.config.nodeId();
        leader.step(new TransferLeadership(1, id));
        assertEquals(new ProposalRejected(1, Reason.INVALID_CHANGE, id),
                leader.notes(ProposalRejected.class).getLast());
        leader.step(new TransferLeadership(2, 17));
        assertEquals(Reason.INVALID_CHANGE, leader.notes(ProposalRejected.class).getLast().reason());
        TestNode follower = cluster.node((id + 1) % 3);
        follower.step(new TransferLeadership(3, id));
        assertEquals(new ProposalRejected(3, Reason.NOT_LEADER, id),
                follower.notes(ProposalRejected.class).getLast());
    }
}
