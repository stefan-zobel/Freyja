package io.github.sz.freyja.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.kv.Command;
import io.github.sz.freyja.common.kv.Command.Alarm;
import io.github.sz.freyja.common.kv.Command.Delete;
import io.github.sz.freyja.common.kv.Command.Put;
import io.github.sz.freyja.common.kv.CommandCodec;
import io.github.sz.freyja.common.kv.CommandResult;
import io.github.sz.freyja.common.kv.CommandResult.Failed;
import io.github.sz.freyja.common.kv.CommandResult.Failure;
import io.github.sz.freyja.common.kv.CommandResult.Written;
import io.github.sz.freyja.lsm.fs.FaultyFileSystem;
import io.github.sz.freyja.node.KvCluster.Member;
import io.github.sz.freyja.node.snapshot.SnapshotTransfers;
import io.github.sz.freyja.node.wire.AlarmCodec;
import io.github.sz.freyja.raft.RaftConfig;
import io.github.sz.freyja.raft.RaftEvent;
import io.github.sz.freyja.raft.Role;

/**
 * The limits that protect a node and the cluster (docs/spec/node.md, sections 4.4 and 7):
 * the alarm for lack of space, deadlines of proposals, and the bounded queue.
 */
class OverloadTest {

    /** The time the nodes of a test see; a test moves it. */
    private final long[] nanos = {0};

    private KvCluster cluster(Node.Limits limits) throws IOException {
        return KvCluster.ofThree(new Node.Options(RaftConfig.defaults(0, 5), 4 * 1024, 256, 1L << 30, 5,
                new SnapshotTransfers.Options(700, 3, 40, 2), limits, () -> nanos[0]));
    }

    /** Proposes a command at the leader and returns what applying it gave. */
    private static CommandResult apply(KvCluster cluster, Command command) {
        cluster.propose(cluster.leader(), command);
        cluster.ticks(2);
        return cluster.results.lastEntry().getValue()[0];
    }

    private static void assertAlarm(KvCluster cluster, Set<Alarm> expected) {
        for (Member member : cluster.members) {
            assertEquals(expected, member.node.store().machine().alarms(), "node " + member.id);
        }
    }

    @Test
    void aStateMachineOverItsQuotaSetsTheAlarmForTheWholeCluster() throws IOException {
        KvCluster cluster = cluster(new Node.Limits(10_000, 30_000, 0, 3, 1L << 40));
        Random random = new Random(1);
        cluster.write(20, 300, random);
        assertAlarm(cluster, Set.of());
        cluster.write(150, 300, random);
        cluster.ticks(5);
        assertAlarm(cluster, Set.of(Alarm.NOSPACE));
        assertTrue(cluster.members.stream().mapToLong(m -> m.node.alarmsRaised()).sum() >= 1);

        byte[] key = KvCluster.b("key-1");
        assertEquals(new Failed(Failure.NO_SPACE), apply(cluster, new Command.Write(cluster.session,
                ++cluster.sequence, List.of(new Put(key, new byte[10])))));
        // The rejected command did not use up its sequence number.
        assertEquals(new Written(), apply(cluster, new Command.Write(cluster.session, cluster.sequence,
                List.of(new Delete(key)))));

        // Cleared by an administrator while the state machine is still too large, the alarm
        // comes back.
        apply(cluster, new Command.ClearAlarm(Alarm.NOSPACE));
        cluster.ticks(8);
        assertAlarm(cluster, Set.of(Alarm.NOSPACE));
        cluster.assertSameState();
        cluster.close();
    }

    @Test
    void aFollowerThatRunsOutOfDiskSpaceAsksTheLeaderForTheAlarm() throws IOException {
        KvCluster cluster = cluster(new Node.Limits(10_000, 1L << 40, 1L << 40, 3, 1L << 40));
        Random random = new Random(2);
        cluster.write(20, 300, random);
        cluster.ticks(10);
        assertAlarm(cluster, Set.of());
        Member leader = cluster.leader();
        Member follower = cluster.members.get((leader.id + 1) % 3);
        // Room enough to go on writing, but less than the node wants to have free.
        follower.memory.setCapacity(1L << 39);
        cluster.ticks(8);
        assertAlarm(cluster, Set.of(Alarm.NOSPACE));
        assertEquals(0, follower.node.alarmsRaised(), "a follower proposes nothing");
        assertTrue(leader.node.alarmsRaised() >= 1);
        long raised = leader.node.alarmsRaised();
        cluster.ticks(20);
        assertEquals(raised, leader.node.alarmsRaised(), "an alarm that is set is not asked for again");

        follower.memory.setCapacity(Long.MAX_VALUE);
        apply(cluster, new Command.ClearAlarm(Alarm.NOSPACE));
        cluster.ticks(10);
        assertAlarm(cluster, Set.of());
        cluster.write(5, 100, random);
        cluster.assertSameState();
        cluster.close();
    }

    @Test
    void aLeaderWithASlowDiskGivesItsLeadershipUp() throws IOException {
        KvCluster cluster = cluster(new Node.Limits(10_000, 1L << 40, 0, 10, 1_000));
        Random random = new Random(3);
        cluster.write(10, 100, random);
        Member slow = cluster.leader();
        // From now on every sync of the log of this node takes long.
        slow.stop();
        FaultyFileSystem faulty = new FaultyFileSystem(slow.memory);
        boolean[] on = {false};
        faulty.setFault((operation, path, index) -> {
            if (on[0] && operation == FaultyFileSystem.Operation.SYNC && path.toString().contains("raft")) {
                nanos[0] += 5_000;
            }
            return null;
        });
        slow.fs = faulty;
        slow.start();
        cluster.ticks(30);
        Member leader = cluster.leader();
        if (leader != slow) {
            leader.node.submit(new RaftEvent.TransferLeadership(1_000_000, slow.id));
            cluster.ticks(10);
        }
        assertEquals(slow, cluster.leader());
        assertEquals(0, slow.node.slowSyncStepDowns());

        on[0] = true;
        cluster.propose(slow, new Command.Write(cluster.session, ++cluster.sequence,
                List.of(new Put(KvCluster.b("k"), new byte[10]))));
        cluster.settle();
        assertEquals(1, slow.node.slowSyncStepDowns());
        assertEquals(Role.FOLLOWER, slow.node.view().role());
        // Followers are not judged: only a leader holds everybody up.
        cluster.ticks(40);
        assertTrue(cluster.leader() != slow || slow.node.slowSyncStepDowns() > 1,
                "the node leads again only to step down again");
        on[0] = false;
        cluster.ticks(40);
        cluster.write(5, 100, random);
        cluster.assertSameState();
        cluster.close();
    }

    @Test
    void aProposalPastItsDeadlineIsDroppedAndAFullQueueRejectsAtOnce() throws IOException {
        KvCluster cluster = KvCluster.ofThree();
        cluster.openSession();
        Member leader = cluster.leader();
        Node node = leader.node;
        long now = node.now();
        assertTrue(now > 0);
        byte[] command = CommandCodec.encode(1, new Command.Write(cluster.session, 1,
                List.of(new Put(KvCluster.b("k"), new byte[5]))));

        assertEquals(Node.Admission.EXPIRED, node.propose(1, command, now - 1));
        assertThrows(IllegalArgumentException.class, () -> node.propose(-1, command, now + 5));

        // Two ticks wait in front of it: when its turn comes, its time is over.
        long applied = leader.applied();
        node.tick();
        node.tick();
        assertEquals(Node.Admission.ACCEPTED, node.propose(2, command, now + 1));
        cluster.settle();
        assertEquals(List.of(2L), leader.expired);
        assertEquals(2, node.expiredProposals());
        assertEquals(applied, leader.applied(), "nothing was proposed");

        // In time, it is proposed and applied.
        assertEquals(Node.Admission.ACCEPTED, node.propose(3, command, node.now() + 1));
        cluster.settle();
        cluster.ticks(1);
        assertEquals(applied + 1, leader.applied());
        assertEquals(List.of(2L), leader.expired);

        // The queue is bounded.
        int accepted = 0;
        while (node.propose(100 + accepted, command, node.now() + 1_000) == Node.Admission.ACCEPTED) {
            accepted++;
        }
        assertEquals(256, accepted);
        assertFalse(node.tick());
        cluster.settle();
        // The burst overran the queues of the followers too; what they dropped is sent again.
        assertTrue(cluster.members.stream().mapToLong(m -> m.node.rejectedEvents()).sum() > 1);
        cluster.ticks(60);
        cluster.assertSameState();
        cluster.close();
    }

    @Test
    void aRequestForAnAlarmSurvivesEncodingAndRejectsDamage() {
        byte[] frame = AlarmCodec.encode(2, 0, Alarm.NOSPACE);
        assertTrue(AlarmCodec.isAlarm(frame));
        assertEquals(new AlarmCodec.Request(2, 0, Alarm.NOSPACE), AlarmCodec.decode(frame));
        for (int bit = 0; bit < frame.length * 8; bit++) {
            byte[] flipped = frame.clone();
            flipped[bit / 8] ^= (byte) (1 << (bit % 8));
            assertThrows(DecodeException.class, () -> AlarmCodec.decode(flipped));
        }
        assertThrows(DecodeException.class, () -> AlarmCodec.decode(new byte[3]));
    }
}
