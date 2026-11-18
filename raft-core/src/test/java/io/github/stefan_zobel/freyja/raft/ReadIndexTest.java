package io.github.stefan_zobel.freyja.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.raft.Notification.ReadIndexReady;
import io.github.stefan_zobel.freyja.raft.Notification.ReadIndexRejected;
import io.github.stefan_zobel.freyja.raft.Notification.Reason;
import io.github.stefan_zobel.freyja.raft.RaftEvent.ClientPropose;
import io.github.stefan_zobel.freyja.raft.RaftEvent.ClientReadIndex;
import io.github.stefan_zobel.freyja.raft.RaftMessage.Heartbeat;

/** ReadIndex (docs/spec/raft.md, section 5). */
class ReadIndexTest {

    private static long heartbeatsTo(TestNode leader, int node) {
        return leader.outbox.stream()
                .filter(o -> o.toNode() == node && o.message() instanceof Heartbeat)
                .count();
    }

    @Test
    void aReadIsConfirmedByAHeartbeatRoundThatStartedAfterIt() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        cluster.tickAll();
        int id = leader.config.nodeId();
        long commit = leader.view().commitIndex();
        int other = (id + 1) % 3;

        leader.step(new ClientReadIndex(1));
        assertTrue(leader.notes(ReadIndexReady.class).isEmpty(), "not before a majority answered");
        assertEquals(1, heartbeatsTo(leader, other), "the request starts a round at once");

        // Further requests share the next round instead of starting one each.
        leader.step(new ClientReadIndex(2));
        leader.step(new ClientReadIndex(3));
        assertEquals(1, heartbeatsTo(leader, other));

        cluster.deliverAll();
        assertEquals(List.of(new ReadIndexReady(1, commit), new ReadIndexReady(2, commit),
                new ReadIndexReady(3, commit)), leader.notes(ReadIndexReady.class));
    }

    @Test
    void aReadIndexCoversEverythingCommittedBeforeTheRequest() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        cluster.tickAll();
        leader.step(new ClientPropose(1, new byte[3]));
        cluster.deliverAll();
        long commit = leader.view().commitIndex();
        leader.step(new ClientReadIndex(9));
        leader.step(new ClientPropose(2, new byte[3])); // after the request: need not be covered
        cluster.deliverAll();
        assertEquals(List.of(new ReadIndexReady(9, commit)), leader.notes(ReadIndexReady.class));
    }

    @Test
    void aLeaderWithoutAMajorityConfirmsNothingAndRejectsWhenItStepsDown() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        cluster.tickAll();
        cluster.isolated.add(leader.config.nodeId());

        leader.step(new ClientReadIndex(5));
        for (int i = 0; i < 8; i++) {
            cluster.tickAll();
        }
        assertTrue(leader.notes(ReadIndexReady.class).isEmpty());
        assertTrue(leader.notes(ReadIndexRejected.class).isEmpty());

        for (int i = 0; i < 25 && leader.view().role() == Role.LEADER; i++) {
            leader.tick(1); // until CheckQuorum finds no majority
        }
        assertEquals(Role.FOLLOWER, leader.view().role());
        assertEquals(List.of(new ReadIndexRejected(5, Reason.NOT_LEADER, RaftNode.NO_NODE)),
                leader.notes(ReadIndexRejected.class));
        assertTrue(leader.notes(ReadIndexReady.class).isEmpty());
    }

    @Test
    void aFollowerAndALeaderWithoutACommitInItsTermReject() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        int id = leader.config.nodeId();
        TestNode follower = cluster.node((id + 1) % 3);
        cluster.tickAll();
        follower.step(new ClientReadIndex(1));
        assertEquals(List.of(new ReadIndexRejected(1, Reason.NOT_LEADER, id)),
                follower.notes(ReadIndexRejected.class));

        // A new leader that has not committed its first entry yet.
        TestNode single = new TestNode(RaftConfig.defaults(0, 1), ClusterConfig.ofVoters(0));
        while (single.view().role() != Role.CANDIDATE) {
            single.tick(1);
        }
        single.persistNext();
        assertEquals(Role.LEADER, single.view().role());
        single.step(new ClientReadIndex(2));
        assertEquals(List.of(new ReadIndexRejected(2, Reason.NOT_READY, 0)),
                single.notes(ReadIndexRejected.class));
        single.persistAll();
        single.step(new ClientReadIndex(3));
        assertEquals(List.of(new ReadIndexReady(3, 1)), single.notes(ReadIndexReady.class),
                "a single node is its own majority");
    }

    @Test
    void anAnswerToAnEarlierRoundDoesNotConfirmALaterRequest() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        cluster.tickAll();
        int id = leader.config.nodeId();

        leader.tick(1); // a round is under way; its answers are held back
        List<Outbound> held = leader.drainOutbox();
        leader.step(new ClientReadIndex(7));
        assertEquals(0, heartbeatsTo(leader, (id + 1) % 3), "the request waits for the next round");

        // The answers to the earlier round arrive: they must not confirm the request.
        for (Outbound outbound : held) {
            cluster.node(outbound.toNode()).receive(id, outbound.message());
        }
        for (TestNode node : cluster.nodes) {
            if (node != leader) {
                for (Outbound answer : node.drainOutbox()) {
                    leader.receive(node.config.nodeId(), answer.message());
                }
            }
        }
        assertTrue(leader.notes(ReadIndexReady.class).isEmpty());
        assertEquals(1, heartbeatsTo(leader, (id + 1) % 3), "the next round starts now");
        cluster.deliverAll();
        assertEquals(1, leader.notes(ReadIndexReady.class).size());
    }
}
