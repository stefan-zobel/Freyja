package io.github.sz.freyja.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.raft.ConfigChange.AddLearner;
import io.github.sz.freyja.raft.ConfigChange.PromoteLearner;
import io.github.sz.freyja.raft.ConfigChange.RemoveLearner;
import io.github.sz.freyja.raft.ConfigChange.RemoveVoter;
import io.github.sz.freyja.raft.Notification.ConfigChanged;
import io.github.sz.freyja.raft.Notification.ProposalAccepted;
import io.github.sz.freyja.raft.Notification.ProposalRejected;
import io.github.sz.freyja.raft.Notification.Reason;
import io.github.sz.freyja.raft.RaftEvent.ClientPropose;
import io.github.sz.freyja.raft.RaftEvent.ProposeConfigChange;
import io.github.sz.freyja.raft.RaftMessage.AppendEntries;

/** Membership change (docs/spec/raft.md, section 6). */
class MembershipTest {

    private static ClusterConfig config(Set<Integer> voters, Set<Integer> learners) {
        return new ClusterConfig(voters, learners);
    }

    private static void change(TestNode node, long requestId, ConfigChange change) {
        node.step(new ProposeConfigChange(requestId, change));
    }

    private static Reason lastRejection(TestNode node) {
        return node.notes(ProposalRejected.class).getLast().reason();
    }

    private static void settle(TestCluster cluster) {
        for (int i = 0; i < 5; i++) {
            cluster.tickAll();
        }
    }

    @Test
    void aConfigurationSurvivesEncodingAndChangesByOneNode() {
        ClusterConfig three = ClusterConfig.ofVoters(0, 1, 2);
        ClusterConfig withLearner = three.with(new AddLearner(7));
        assertEquals(config(Set.of(0, 1, 2), Set.of(7)), withLearner);
        assertEquals(withLearner, ClusterConfig.decode(withLearner.encode()));
        assertEquals(ClusterConfig.EMPTY, ClusterConfig.decode(ClusterConfig.EMPTY.encode()));
        assertEquals(config(Set.of(0, 1, 2, 7), Set.of()), withLearner.with(new PromoteLearner(7)));
        assertEquals(three, withLearner.with(new RemoveLearner(7)));
        assertEquals(ClusterConfig.ofVoters(0, 2), three.with(new RemoveVoter(1)));

        assertNull(three.with(new AddLearner(1)), "already a voter");
        assertNull(withLearner.with(new AddLearner(7)), "already a learner");
        assertNull(three.with(new PromoteLearner(7)), "not a learner");
        assertNull(three.with(new RemoveVoter(7)), "not a voter");
        assertNull(three.with(new RemoveLearner(1)), "not a learner");
        assertNull(ClusterConfig.ofVoters(4).with(new RemoveVoter(4)), "the last voter stays");
        assertThrows(IllegalArgumentException.class, () -> ClusterConfig.decode(new byte[] {1, 0, 0}));
        assertThrows(IllegalArgumentException.class,
                () -> ClusterConfig.decode(new byte[] {2, 0, 0, 0, 5, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0}));
    }

    @Test
    void aNodeJoinsAsLearnerAndIsPromotedToVoter() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        settle(cluster);
        TestNode joiner = cluster.addEmptyNode();
        assertEquals(ClusterConfig.EMPTY, joiner.view().config());
        joiner.tick(100);
        assertEquals(Role.FOLLOWER, joiner.view().role(), "a node that is no voter starts no election");

        change(leader, 1, new AddLearner(3));
        assertEquals(1, leader.notes(ProposalAccepted.class).size());
        assertEquals(config(Set.of(0, 1, 2), Set.of(3)), leader.view().config(),
                "in force as soon as it is in the log");
        settle(cluster);
        for (TestNode node : cluster.nodes) {
            assertEquals(config(Set.of(0, 1, 2), Set.of(3)), node.view().config());
            assertEquals(leader.view().commitIndex(), node.view().commitIndex());
            assertEquals(leader.applied.size(), node.applied.size());
        }
        assertEquals(new ConfigChanged(leader.view().lastLogIndex(), leader.view().config()),
                joiner.notes(ConfigChanged.class).getLast());

        // A learner does not count toward the majority: with both other voters cut off,
        // nothing commits although the learner acknowledges.
        long commit = leader.view().commitIndex();
        int id = leader.config.nodeId();
        cluster.isolated.addAll(Set.of((id + 1) % 3, (id + 2) % 3));
        leader.step(new ClientPropose(9, new byte[1]));
        cluster.deliverAll();
        assertEquals(commit, leader.view().commitIndex());
        assertEquals(leader.view().lastLogIndex(), joiner.view().lastLogIndex());
        cluster.isolated.clear();
        settle(cluster);

        change(leader, 2, new PromoteLearner(3));
        settle(cluster);
        ClusterConfig four = config(Set.of(0, 1, 2, 3), Set.of());
        for (TestNode node : cluster.nodes) {
            assertEquals(four, node.view().config());
        }
        assertEquals(3, four.quorum());
        // Two of four voters are no majority any more.
        commit = leader.view().commitIndex();
        cluster.isolated.addAll(Set.of((id + 1) % 3, (id + 2) % 3));
        leader.step(new ClientPropose(10, new byte[1]));
        cluster.deliverAll();
        assertEquals(commit, leader.view().commitIndex());
    }

    @Test
    void changesAreRejectedWhenTheyAreNotSafeOrDoNotFit() {
        TestCluster cluster = new TestCluster(3, RaftConfig.defaults(0, 0).withPromotionLag(2));
        TestNode leader = cluster.electLeader();
        int id = leader.config.nodeId();
        TestNode follower = cluster.node((id + 1) % 3);
        settle(cluster);

        change(follower, 1, new AddLearner(3));
        assertEquals(new ProposalRejected(1, Reason.NOT_LEADER, id),
                follower.notes(ProposalRejected.class).getLast());
        change(leader, 2, new PromoteLearner(3));
        assertEquals(Reason.INVALID_CHANGE, lastRejection(leader));
        change(leader, 3, new RemoveVoter(9));
        assertEquals(Reason.INVALID_CHANGE, lastRejection(leader));

        // One change at a time: the first one is not committed while the followers are away.
        cluster.addEmptyNode();
        cluster.isolated.addAll(Set.of((id + 1) % 3, (id + 2) % 3));
        change(leader, 4, new AddLearner(3));
        assertEquals(1, leader.notes(ProposalAccepted.class).size());
        change(leader, 5, new RemoveVoter((id + 1) % 3));
        assertEquals(Reason.CONFIG_IN_PROGRESS, lastRejection(leader));
        cluster.isolated.clear();
        settle(cluster);

        // A learner that is too far behind is not promoted.
        cluster.isolated.add(3);
        for (int i = 0; i < 3; i++) {
            leader.step(new ClientPropose(20 + i, new byte[1]));
        }
        cluster.deliverAll();
        change(leader, 6, new PromoteLearner(3));
        assertEquals(Reason.LEARNER_BEHIND, lastRejection(leader));
        cluster.isolated.clear();
        for (int i = 0; i < 30; i++) {
            cluster.tickAll(); // the appends that were lost are sent again after an election timeout
        }
        change(leader, 7, new PromoteLearner(3));
        assertEquals(7, leader.notes(ProposalAccepted.class).getLast().requestId(),
                "rejections: " + leader.notes(ProposalRejected.class));
    }

    @Test
    void aNewLeaderChangesNothingBeforeItHasCommittedAnEntryOfItsTerm() {
        TestNode single = new TestNode(RaftConfig.defaults(0, 1), ClusterConfig.ofVoters(0));
        while (single.view().role() != Role.CANDIDATE) {
            single.tick(1);
        }
        single.persistNext();
        assertEquals(Role.LEADER, single.view().role());
        change(single, 1, new AddLearner(1));
        assertEquals(Reason.NOT_READY, lastRejection(single));
        single.persistAll();
        change(single, 2, new AddLearner(1));
        assertEquals(1, single.notes(ProposalAccepted.class).size());
    }

    @Test
    void aRemovedVoterNoLongerCountsAndNoLongerReceives() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        settle(cluster);
        int id = leader.config.nodeId();
        int removed = (id + 1) % 3;
        int kept = (id + 2) % 3;

        change(leader, 1, new RemoveVoter(removed));
        settle(cluster);
        assertEquals(ClusterConfig.ofVoters(id, kept), leader.view().config());
        assertEquals(leader.view().lastLogIndex(), leader.view().commitIndex());

        // Both remaining voters are needed now.
        long commit = leader.view().commitIndex();
        cluster.isolated.add(kept);
        leader.step(new ClientPropose(5, new byte[1]));
        cluster.deliverAll();
        assertEquals(commit, leader.view().commitIndex());
        assertTrue(leader.outbox.stream().noneMatch(o -> o.toNode() == removed));
        leader.tick(3);
        assertTrue(leader.outbox.stream().noneMatch(o -> o.toNode() == removed), "not even heartbeats");
    }

    @Test
    void aLeaderThatRemovesItselfStepsDownOnceTheChangeIsCommitted() {
        TestCluster cluster = new TestCluster(3);
        TestNode leader = cluster.electLeader();
        settle(cluster);
        int id = leader.config.nodeId();
        long term = leader.view().term();

        cluster.isolated.addAll(Set.of((id + 1) % 3, (id + 2) % 3));
        change(leader, 1, new RemoveVoter(id));
        assertEquals(Role.LEADER, leader.view().role(), "it leads until the change is committed");
        assertEquals(ClusterConfig.ofVoters((id + 1) % 3, (id + 2) % 3), leader.view().config());
        cluster.isolated.clear();
        cluster.deliverAll();
        assertEquals(Role.FOLLOWER, leader.view().role());
        assertEquals(leader.view().lastLogIndex(), leader.view().commitIndex());

        TestNode successor = cluster.electLeader();
        assertNotEquals(leader, successor);
        assertTrue(successor.view().term() > term);
        leader.tick(200);
        assertEquals(Role.FOLLOWER, leader.view().role(), "a removed node starts no election");
        successor.step(new ClientPropose(7, new byte[1]));
        cluster.deliverAll();
        assertEquals(successor.view().lastLogIndex(), successor.view().commitIndex());
    }

    @Test
    void aConfigurationInATruncatedEntryIsRevertedAndTheLogRestoresItAfterARestart() {
        ClusterConfig three = ClusterConfig.ofVoters(0, 1, 2);
        ClusterConfig grown = three.with(new AddLearner(3));
        TestNode node = new TestNode(RaftConfig.defaults(1, 7), three);
        LogEntry command = new LogEntry(1, 1, EntryType.COMMAND, new byte[1]);
        LogEntry configEntry = new LogEntry(2, 1, EntryType.CONFIG, grown.encode());
        node.receive(0, new AppendEntries(1, 0, 0, List.of(command, configEntry), 0));
        assertEquals(grown, node.view().config());
        assertEquals(List.of(new ConfigChanged(2, grown)), node.notes(ConfigChanged.class));
        node.persistAll();

        node.restart();
        assertEquals(grown, node.view().config(), "read from the stored log");

        // A leader of a later term replaces the entry: the earlier configuration is back.
        node.receive(2, new AppendEntries(2, 1, 1,
                List.of(new LogEntry(2, 2, EntryType.NOOP, new byte[0])), 0));
        assertEquals(three, node.view().config());
        assertEquals(new ConfigChanged(0, three), node.notes(ConfigChanged.class).getLast());
        node.persistAll();
        node.restart();
        assertEquals(three, node.view().config());
    }
}
