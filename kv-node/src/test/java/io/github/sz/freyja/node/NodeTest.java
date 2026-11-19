package io.github.sz.freyja.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.common.kv.CommandResult;
import io.github.sz.freyja.lsm.fs.FaultyFileSystem;
import io.github.sz.freyja.lsm.fs.MemoryFileSystem;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.node.raftlog.RaftLogStore;
import io.github.sz.freyja.node.store.HardStateStore;
import io.github.sz.freyja.node.store.LostIdentityException;
import io.github.sz.freyja.node.store.NodeIdentity;
import io.github.sz.freyja.node.wire.MessageCodec;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.EntryType;
import io.github.sz.freyja.raft.LogEntry;
import io.github.sz.freyja.raft.Notification;
import io.github.sz.freyja.raft.RaftConfig;
import io.github.sz.freyja.raft.RaftEvent;
import io.github.sz.freyja.raft.RaftMessage;
import io.github.sz.freyja.raft.Role;

/**
 * Complete nodes on in-memory file systems, joined by a transport that the test steps
 * (docs/spec/node.md, sections 1 and 3).
 */
class NodeTest {

    private static final long CLUSTER = 0xFEEDL;
    private static final ClusterConfig THREE = ClusterConfig.ofVoters(0, 1, 2);
    private static final Path DATA = Path.of("data");

    /** A state machine that lives in memory: it is rebuilt from the log after a restart. */
    private static final class ListMachine implements StateMachine {
        final List<String> applied = new ArrayList<>();
        long index;

        @Override
        public long appliedIndex() {
            return index;
        }

        @Override
        public List<CommandResult> apply(List<LogEntry> entries) {
            List<CommandResult> results = new ArrayList<>();
            for (LogEntry entry : entries) {
                assertEquals(index + 1, entry.index(), "entries are applied in order, each once");
                index = entry.index();
                applied.add(entry.index() + "@" + entry.term() + ":" + entry.type() + ":"
                        + Arrays.hashCode(entry.payload()));
                results.add(null);
            }
            return results;
        }
    }

    /** One node of the test cluster with everything that survives a restart, and what does not. */
    private final class Member {
        final int id;
        final MemoryFileSystem memory = new MemoryFileSystem();
        StorageFileSystem fs = memory;
        Node node;
        ListMachine machine;
        final List<Notification> notes = new ArrayList<>();

        Member(int id) throws IOException {
            this.id = id;
            Node.initialize(memory, DATA, new NodeIdentity(CLUSTER, id));
        }

        void start() throws IOException {
            machine = new ListMachine();
            node = Node.open(fs, DATA, options, THREE, machine, (to, message) -> network.add(message),
                    notes::add);
        }

        void stop() throws IOException {
            node.close();
            node = null;
        }
    }

    private final Node.Options options = new Node.Options(RaftConfig.defaults(0, 5), 4096, 256);
    private final ArrayDeque<byte[]> network = new ArrayDeque<>();
    private final List<Member> members = new ArrayList<>();

    private void startCluster() throws IOException {
        for (int id = 0; id < 3; id++) {
            Member member = new Member(id);
            members.add(member);
            member.start();
        }
    }

    /** Delivers every message on its way and lets every node work until all are idle. */
    private void settle() throws IOException {
        boolean progress = true;
        while (progress) {
            progress = false;
            while (!network.isEmpty()) {
                byte[] message = network.pollFirst();
                Member target = members.get(MessageCodec.decode(message).to());
                if (target.node != null) {
                    target.node.offer(message);
                }
                progress = true;
            }
            for (Member member : members) {
                while (member.node != null && member.node.runOnce()) {
                    progress = true;
                }
            }
        }
    }

    private void ticks(int count) throws IOException {
        for (int i = 0; i < count; i++) {
            for (Member member : members) {
                if (member.node != null) {
                    assertTrue(member.node.tick());
                }
            }
            settle();
        }
    }

    private Member leader() throws IOException {
        for (int i = 0; i < 300; i++) {
            Member leader = null;
            for (Member member : members) {
                if (member.node != null && member.node.view().role() == Role.LEADER
                        && (leader == null || member.node.view().term() > leader.node.view().term())) {
                    leader = member;
                }
            }
            if (leader != null) {
                return leader;
            }
            ticks(1);
        }
        throw new AssertionError("no leader");
    }

    private void propose(Member member, long requestId, int size) {
        byte[] command = new byte[size];
        Arrays.fill(command, (byte) requestId);
        assertTrue(member.node.submit(new RaftEvent.ClientPropose(requestId, command)));
    }

    @Test
    void threeNodesElectALeaderReplicateAndApplyTheSameEntries() throws IOException {
        startCluster();
        Member leader = leader();
        for (int i = 0; i < 30; i++) {
            propose(leader, i, 300);
            settle();
        }
        ticks(3);
        assertEquals(31, leader.machine.applied.size(), "the entry of the term and 30 commands");
        for (Member member : members) {
            assertEquals(leader.machine.applied, member.machine.applied);
            assertEquals(leader.node.view().term(), member.node.view().term());
            assertEquals(0, member.node.invalidMessages());
            assertEquals(0, member.node.rejectedEvents());
        }
        assertEquals(30, leader.notes.stream().filter(n -> n instanceof Notification.ProposalAccepted).count());
        // Several segments were written.
        assertTrue(leader.memory.list(DATA.resolve(Node.RAFT_DIRECTORY)).size() > 2);
        for (Member member : members) {
            member.stop();
            assertEquals(0, member.memory.openHandleCount());
        }
    }

    @Test
    void aRestartedNodeKeepsItsTermItsVoteAndItsLog() throws IOException {
        startCluster();
        Member leader = leader();
        for (int i = 0; i < 10; i++) {
            propose(leader, i, 100);
            settle();
        }
        ticks(2);
        Member follower = members.get((leader.id + 1) % 3);
        long term = follower.node.view().term();
        int vote = follower.node.view().votedFor();
        List<String> before = new ArrayList<>(follower.machine.applied);

        follower.stop();
        follower.start();
        assertEquals(term, follower.node.view().term());
        assertEquals(vote, follower.node.view().votedFor());
        assertEquals(11, follower.node.view().lastLogIndex());
        assertTrue(follower.machine.applied.isEmpty(), "the state machine of this test lives in memory");
        ticks(3);
        assertEquals(before, follower.machine.applied, "replayed from the log");
        assertEquals(leader, leader(), "the restart did not disturb the leader");
    }

    @Test
    void nothingAppliedAnywhereIsLostWhenNodesCrash() throws IOException {
        startCluster();
        Random random = new Random(21);
        List<String> appliedAnywhere = new ArrayList<>();
        for (int round = 0; round < 12; round++) {
            Member leader = leader();
            for (int i = 0; i < 6; i++) {
                propose(leader, round * 100 + i, 200);
                if (random.nextBoolean()) {
                    settle();
                }
            }
            if (random.nextBoolean()) {
                settle();
            }
            for (Member member : members) {
                if (member.node != null && member.machine.applied.size() > appliedAnywhere.size()) {
                    appliedAnywhere = new ArrayList<>(member.machine.applied);
                }
            }
            // A node crashes: what was not synced is lost in part, garbled, or kept.
            Member victim = members.get(random.nextInt(3));
            victim.node.close();
            victim.node = null;
            victim.memory.crash(random);
            ticks(random.nextInt(30));
            victim.start();
            ticks(25);
        }
        Member leader = leader();
        ticks(5);
        for (Member member : members) {
            assertEquals(leader.machine.applied, member.machine.applied);
            assertFalse(member.node.view().logRecovering(), "a crash is not damage");
        }
        assertEquals(appliedAnywhere, leader.machine.applied.subList(0, appliedAnywhere.size()),
                "what any node had applied is still there, in the same order");
        assertTrue(appliedAnywhere.size() > 40, "applied: " + appliedAnywhere.size());
    }

    @Test
    void aNodeWithADamagedLogSetsItAsideRecoversThroughTheLeaderAndAppliesNothingTwice()
            throws IOException {
        startCluster();
        Member leader = leader();
        for (int i = 0; i < 30; i++) {
            propose(leader, i, 300);
            settle();
        }
        ticks(2);
        Member victim = members.get((leader.id + 1) % 3);
        long epoch = victim.node.view().logEpoch();
        victim.stop();

        Path raft = DATA.resolve(Node.RAFT_DIRECTORY);
        List<Path> segments = victim.memory.list(raft).stream()
                .filter(p -> p.getFileName().toString().endsWith(".rlog")).sorted().toList();
        assertTrue(segments.size() >= 3, segments.toString());
        Path damaged = segments.get(1);
        byte[] one = victim.memory.peek(damaged, 300, 1);
        one[0] ^= 0x08;
        victim.memory.overwrite(damaged, 300, one);

        victim.start();
        assertTrue(victim.node.view().logRecovering());
        assertEquals(epoch + 1, victim.node.view().logEpoch());
        assertTrue(HardStateStore.open(victim.memory, DATA).get().logRecovering(), "durable before anything else");
        assertTrue(victim.node.view().lastLogIndex() < 31);
        List<Path> aside = victim.memory.list(raft.resolve(RaftLogStore.DAMAGED_DIRECTORY));
        assertEquals(segments.size() - 1, aside.size(), "the damaged segment and all after it are kept aside");

        ticks(40);
        assertFalse(victim.node.view().logRecovering(), "the leader has refilled the log");
        assertEquals(leader.node.view().lastLogIndex(), victim.node.view().lastLogIndex());
        assertEquals(leader.machine.applied, victim.machine.applied);

        // The same again, but this time the node crashes between finding the damage and
        // setting the segments aside: the damage is found again.
        victim.stop();
        segments = victim.memory.list(raft).stream()
                .filter(p -> p.getFileName().toString().endsWith(".rlog")).sorted().toList();
        Path first = segments.getFirst();
        byte[] byteOfFirst = victim.memory.peek(first, 100, 1);
        byteOfFirst[0] ^= 0x01;
        victim.memory.overwrite(first, 100, byteOfFirst);
        FaultyFileSystem faulty = new FaultyFileSystem(victim.memory);
        faulty.setFault((operation, path, index) -> operation == FaultyFileSystem.Operation.RENAME
                && path.toString().contains(RaftLogStore.DAMAGED_DIRECTORY) ? new IOException("injected") : null);
        victim.fs = faulty;
        assertThrows(IOException.class, victim::start);
        victim.fs = victim.memory;
        victim.start();
        assertTrue(victim.node.view().logRecovering());
        assertEquals(0, victim.node.view().lastLogIndex(), "the first segment was damaged: nothing is vouched for");
        ticks(60);
        assertFalse(victim.node.view().logRecovering());
        assertEquals(leader.machine.applied, victim.machine.applied);
    }

    @Test
    void aNodeLeavesLogRecoveringModeOnlyWithTheEntriesThatEndItDurable() throws IOException {
        startCluster();
        Member leader = leader();
        for (int i = 0; i < 30; i++) {
            propose(leader, i, 300);
            settle();
        }
        ticks(2);
        Member victim = members.get((leader.id + 1) % 3);
        long acknowledged = victim.node.view().lastLogIndex();
        victim.stop();

        Path raft = DATA.resolve(Node.RAFT_DIRECTORY);
        List<Path> segments = victim.memory.list(raft).stream()
                .filter(p -> p.getFileName().toString().endsWith(".rlog")).sorted().toList();
        Path damaged = segments.get(1);
        byte[] one = victim.memory.peek(damaged, 300, 1);
        one[0] ^= 0x08;
        victim.memory.overwrite(damaged, 300, one);

        // The sync of the log fails in the step in which the core leaves log-recovering mode,
        // and what was not synced is lost.
        boolean[] failed = new boolean[1];
        FaultyFileSystem faulty = new FaultyFileSystem(victim.memory);
        faulty.setFault((operation, path, index) -> {
            if (!failed[0] && operation == FaultyFileSystem.Operation.SYNC
                    && path.getFileName().toString().endsWith(".rlog")
                    && victim.node != null && !victim.node.view().logRecovering()) {
                failed[0] = true;
                return new IOException("injected");
            }
            return null;
        });
        victim.fs = faulty;
        victim.start();
        assertTrue(victim.node.view().logRecovering());
        assertThrows(IOException.class, () -> ticks(40));
        assertTrue(failed[0], "the step that ends log-recovering mode wrote to the log");
        victim.node = null;
        victim.memory.crash();

        victim.fs = victim.memory;
        victim.start();
        assertTrue(victim.node.view().logRecovering() || victim.node.view().lastLogIndex() >= acknowledged,
                "a node out of log-recovering mode holds what it acknowledged before the damage: "
                        + victim.node.view().lastLogIndex() + " < " + acknowledged);
        ticks(60);
        assertFalse(victim.node.view().logRecovering());
        assertEquals(leader.machine.applied, victim.machine.applied);
    }

    @Test
    void aNodeThatHasLostItsIdentityRefusesToStart() throws IOException {
        Member member = new Member(0);
        members.add(member);
        member.start();
        member.stop();
        byte[] a = member.memory.peek(DATA.resolve(HardStateStore.FILE_A), 0, 10);
        a[9] ^= 1;
        member.memory.overwrite(DATA.resolve(HardStateStore.FILE_A), 0, a);
        member.start(); // one hard-state file is enough
        member.stop();
        member.memory.overwrite(DATA.resolve(HardStateStore.FILE_A), 0, a);
        byte[] b = member.memory.peek(DATA.resolve(HardStateStore.FILE_B), 0, 10);
        b[9] ^= 1;
        member.memory.overwrite(DATA.resolve(HardStateStore.FILE_B), 0, b);
        assertThrows(LostIdentityException.class, member::start);

        Member other = new Member(1);
        other.memory.delete(DATA.resolve(NodeIdentity.FILE_NAME));
        assertThrows(LostIdentityException.class, other::start);
        assertThrows(IOException.class, () -> Node.initialize(member.memory, DATA, new NodeIdentity(CLUSTER, 9)),
                "a directory is prepared once");
        assertEquals(0, member.memory.openHandleCount());
        assertEquals(0, other.memory.openHandleCount());
    }

    @Test
    void damagedAndMisdirectedMessagesAreDroppedAndAFullQueueRejects() throws IOException {
        Member member = new Member(0);
        members.add(member);
        member.start();
        byte[] message = MessageCodec.encode(1, 0, new RaftMessage.Heartbeat(1, 0, 1));
        assertTrue(member.node.offer(message));
        byte[] damaged = message.clone();
        damaged[10] ^= 4;
        assertFalse(member.node.offer(damaged));
        assertFalse(member.node.offer(MessageCodec.encode(1, 2, new RaftMessage.Heartbeat(1, 0, 1))),
                "meant for another node");
        assertEquals(2, member.node.invalidMessages());

        int accepted = 1;
        while (member.node.tick()) {
            accepted++;
        }
        assertEquals(256, accepted, "the queue is bounded");
        assertEquals(1, member.node.rejectedEvents());
        assertFalse(member.node.submit(new RaftEvent.ClientPropose(1, new byte[1])));
        while (member.node.runOnce()) {
            // work the queue off
        }
        assertEquals(0, member.node.waitingEvents());
        assertTrue(member.node.tick());
        assertNotNull(member.node.view());
    }

    @Test
    void aStorageFailureStopsTheNode() throws IOException {
        Member member = new Member(0);
        members.add(member);
        FaultyFileSystem faulty = new FaultyFileSystem(member.memory);
        member.fs = faulty;
        member.start();
        faulty.setFault((operation, path, index) ->
                operation == FaultyFileSystem.Operation.SYNC ? new IOException("injected") : null);
        // A vote request makes the node store a new term.
        member.node.offer(MessageCodec.encode(1, 0, new RaftMessage.RequestVote(5, 0, 0, false)));
        assertThrows(IOException.class, member.node::runOnce);
        assertTrue(member.node.stopped());
        assertFalse(member.node.tick());
        assertThrows(IOException.class, member.node::runOnce);
        assertTrue(network.isEmpty(), "no vote left the node");
        member.node.close();

        faulty.setFault(null);
        member.start();
        assertEquals(EntryType.values().length, 3);
        assertTrue(member.node.view().term() == 0 || member.node.view().term() == 5,
                "the old or the new term, whichever reached the disk");
    }

    /** Runs a sync only when the test has let it. */
    private static final class HeldSyncer implements LogSyncer {
        Runnable pending;
        boolean released;
        /** Every sync runs as soon as the node asks for it. */
        boolean free;
        int started;

        @Override
        public void start(Runnable sync) {
            assertNull(pending, "one sync at a time");
            pending = sync;
            released = false;
            started++;
        }

        @Override
        public boolean done() {
            if (pending != null && (released || free)) {
                await();
            }
            return pending == null;
        }

        @Override
        public void await() {
            Runnable sync = pending;
            pending = null;
            if (sync != null) {
                sync.run();
            }
        }
    }

    @Test
    void aLeaderGoesOnWhileItsLogSyncsAndAppliesOnlyWhatIsDurable() throws IOException {
        startCluster();
        Member leader = leader();
        ticks(2);
        HeldSyncer held = new HeldSyncer();
        leader.node.syncer(held);
        int applied = leader.machine.applied.size();

        propose(leader, 1, 100);
        settle();
        assertNotNull(held.pending, "the sync of the leader runs");
        long first = leader.node.view().lastLogIndex();
        assertEquals(first, leader.node.view().commitIndex(), "the followers made it durable: it is committed");
        assertEquals(applied, leader.machine.applied.size(), "the leader applies only what it holds durably");

        propose(leader, 2, 100);
        settle();
        assertEquals(1, held.started, "the entry waits for the next sync");
        assertEquals(1, leader.node.appendsDuringSync());
        held.released = true;
        assertTrue(leader.node.runOnce(), "the end of a sync is work");
        assertEquals(2, held.started, "the next sync covers what was appended meanwhile");
        assertEquals(applied + 1, leader.machine.applied.size());
        held.released = true;
        settle();
        assertFalse(leader.node.appendsUnsynced());
        assertEquals(applied + 2, leader.machine.applied.size());

        // A new term is stored only after what was appended before it is durable.
        propose(leader, 3, 100);
        settle();
        assertNotNull(held.pending);
        long term = leader.node.view().term();
        int other = (leader.id + 1) % 3;
        assertTrue(leader.node.offer(MessageCodec.encode(other, leader.id,
                new RaftMessage.RequestVote(term + 1, 0, 0, false, true))));
        assertTrue(leader.node.runOnce());
        assertEquals(1, leader.node.syncsAwaited());
        assertNull(held.pending);
        assertFalse(leader.node.appendsUnsynced());
        assertEquals(term + 1, leader.node.view().term());

        held.free = true;
        Member next = leader();
        propose(next, 4, 100);
        ticks(5);
        for (Member member : members) {
            assertEquals(next.machine.applied, member.machine.applied);
        }
        assertTrue(next.machine.applied.size() > applied + 3, "the entries of the old leader are applied");
        for (Member member : members) {
            member.stop();
            assertEquals(0, member.memory.openHandleCount());
        }
    }

    @Test
    void aFailedSyncAwayFromTheLoopStopsTheNodeBeforeItAcknowledges() throws IOException {
        startCluster();
        Member leader = leader();
        ticks(2);
        Member follower = members.get((leader.id + 1) % 3);
        follower.stop();
        FaultyFileSystem faulty = new FaultyFileSystem(follower.memory);
        follower.fs = faulty;
        follower.start();
        HeldSyncer held = new HeldSyncer();
        follower.node.syncer(held);
        ticks(3);
        long acknowledged = follower.node.view().lastLogIndex();

        faulty.setFault((operation, path, index) ->
                operation == FaultyFileSystem.Operation.SYNC ? new IOException("injected") : null);
        propose(leader, 1, 100);
        settle();
        assertNotNull(held.pending, "the follower has appended the entry and syncs it");
        held.released = true;
        assertThrows(IOException.class, follower.node::runOnce);
        assertTrue(follower.node.stopped());
        assertEquals(acknowledged, leader.node.peers().stream().filter(p -> p.nodeId() == follower.id)
                .findFirst().orElseThrow().matchIndex(), "the follower acknowledged nothing it failed to sync");
        follower.node.close();
        follower.node = null;
        settle();
        assertEquals(leader.node.view().lastLogIndex(), leader.node.view().commitIndex(),
                "the other follower is enough");
    }
}
