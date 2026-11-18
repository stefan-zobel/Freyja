package io.github.stefan_zobel.freyja.node;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

import io.github.stefan_zobel.freyja.common.kv.Command;
import io.github.stefan_zobel.freyja.common.kv.Command.Put;
import io.github.stefan_zobel.freyja.common.kv.CommandCodec;
import io.github.stefan_zobel.freyja.common.kv.CommandResult;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.SessionRegistered;
import io.github.stefan_zobel.freyja.lsm.EngineOptions;
import io.github.stefan_zobel.freyja.lsm.fs.MemoryFileSystem;
import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;
import io.github.stefan_zobel.freyja.node.sm.StateMachineStore;
import io.github.stefan_zobel.freyja.node.snapshot.SnapshotTransfers;
import io.github.stefan_zobel.freyja.node.store.NodeIdentity;
import io.github.stefan_zobel.freyja.node.wire.MessageCodec;
import io.github.stefan_zobel.freyja.raft.ClusterConfig;
import io.github.stefan_zobel.freyja.raft.LogEntry;
import io.github.stefan_zobel.freyja.raft.Notification;
import io.github.stefan_zobel.freyja.raft.RaftConfig;
import io.github.stefan_zobel.freyja.raft.RaftEvent;
import io.github.stefan_zobel.freyja.raft.Role;
import io.github.stefan_zobel.freyja.raft.SnapshotMeta;

/**
 * Complete nodes in one process, each on its own in-memory file system, joined by a network
 * that the test steps. Small segments and a small snapshot threshold make logs get shortened
 * after a few dozen commands.
 */
final class KvCluster {

    static final Path DATA = Path.of("data");

    final class Member implements NodeListener {
        final int id;
        final MemoryFileSystem memory = new MemoryFileSystem();
        /** The file system the node is started on; a test may put a faulty one in front. */
        StorageFileSystem fs = memory;
        Node node;
        final List<Notification> notes = new ArrayList<>();
        final List<SnapshotMeta> installed = new ArrayList<>();
        final List<Long> expired = new ArrayList<>();

        Member(int id, ClusterConfig config) throws IOException {
            this.id = id;
            Node.initialize(memory, DATA, new NodeIdentity(7, id), config, 1);
        }

        void start() throws IOException {
            node = Node.open(fs, DATA, options, new StateMachineStore.Options(new EngineOptions()
                    .deterministic(true).reserveSize(0).minFreeSpace(0).memTableSize(32 * 1024)
                    .blockCacheCapacity(1024 * 1024), 60_000), (to, message) -> network.add(message), this);
        }

        boolean running() {
            return node != null;
        }

        void stop() throws IOException {
            node.close();
            node = null;
        }

        /** The process dies: what was not synced is lost in part, garbled, or kept. */
        void crash(Random random) {
            Node dead = node;
            node = null;
            memory.crash(random);
            try {
                dead.close(); // only releases the memory of the dead instance
            } catch (IOException | RuntimeException filesAreGone) {
                // expected
            }
        }

        long digest() throws IOException {
            return node.store().machine().digest();
        }

        long applied() {
            return node.store().appliedIndex();
        }

        @Override
        public void notification(Notification notification) {
            notes.add(notification);
        }

        @Override
        public void applied(LogEntry entry, CommandResult result) {
            // Every node, every time it applies an entry, gets the same result.
            CommandResult[] known = results.putIfAbsent(entry.index(), new CommandResult[] {result});
            if (known != null) {
                assertEquals(known[0], result, "entry " + entry.index() + " on node " + id);
            }
        }

        @Override
        public void snapshotInstalled(SnapshotMeta meta) {
            installed.add(meta);
        }

        @Override
        public void proposalExpired(long requestId) {
            expired.add(requestId);
        }
    }

    Node.Options options;
    final ArrayDeque<byte[]> network = new ArrayDeque<>();
    final List<Member> members = new ArrayList<>();
    /** The result of every applied entry, by index; an array so that null can be stored. */
    final TreeMap<Long, CommandResult[]> results = new TreeMap<>();
    /** Nodes that get no messages and whose messages get nowhere. */
    final Set<Integer> isolated = new HashSet<>();
    /** Decides for each frame whether the network loses it; {@code null} loses nothing. */
    Predicate<byte[]> lose;
    /** Decides for each frame whether the network delivers it twice. */
    Predicate<byte[]> duplicate;
    /** Members whose node stopped with a failure during the last steps. */
    final List<Member> failed = new ArrayList<>();
    long delivered;

    private long time = 1_000;
    private long requestId;
    long session;
    long sequence;

    /** Adds a member that knows the given configuration from its creation. */
    Member add(ClusterConfig config) throws IOException {
        Member member = new Member(members.size(), config);
        members.add(member);
        return member;
    }

    static KvCluster ofThree() throws IOException {
        return ofThree(16 * 1024);
    }

    /** Three nodes that shorten their logs when these take more than the given bytes. */
    static KvCluster ofThree(long snapshotLogBytes) throws IOException {
        return ofThree(new Node.Options(RaftConfig.defaults(0, 5), 4 * 1024, 256, snapshotLogBytes, 5,
                new SnapshotTransfers.Options(700, 3, 40, 2), new Node.Limits(10_000, 1L << 30, 0, 10)));
    }

    static KvCluster ofThree(Node.Options options) throws IOException {
        KvCluster cluster = new KvCluster();
        cluster.options = options;
        for (int i = 0; i < 3; i++) {
            cluster.add(ClusterConfig.ofVoters(0, 1, 2)).start();
        }
        return cluster;
    }

    /** Delivers every message on its way and lets every node work until all are idle. */
    void settle() {
        boolean progress = true;
        while (progress) {
            progress = false;
            while (!network.isEmpty()) {
                byte[] message = network.pollFirst();
                progress = true;
                int to = MessageCodec.addressee(message);
                int from = to < 0 ? -1 : io.github.stefan_zobel.freyja.common.LittleEndian.getInt32(message, 2);
                if (to < 0 || to >= members.size() || isolated.contains(to) || isolated.contains(from)
                        || (lose != null && lose.test(message))) {
                    continue;
                }
                Member target = members.get(to);
                if (target.running()) {
                    delivered++;
                    target.node.offer(message);
                    if (duplicate != null && duplicate.test(message)) {
                        target.node.offer(message.clone());
                    }
                }
            }
            for (Member member : members) {
                try {
                    while (member.running() && member.node.runOnce()) {
                        progress = true;
                    }
                } catch (IOException e) {
                    failed.add(member);
                    try {
                        member.node.close();
                    } catch (IOException ignored) {
                        // the node is dead anyway
                    }
                    member.node = null;
                }
            }
        }
    }

    void ticks(int count) {
        for (int i = 0; i < count; i++) {
            for (Member member : members) {
                if (member.running()) {
                    member.node.tick();
                }
            }
            settle();
        }
    }

    Member leader() {
        for (int i = 0; i < 600; i++) {
            Member leader = null;
            for (Member member : members) {
                if (member.running() && !isolated.contains(member.id)
                        && member.node.view().role() == Role.LEADER
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

    static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    void propose(Member member, Command command) {
        member.node.submit(new RaftEvent.ClientPropose(++requestId, CommandCodec.encode(time += 10, command)));
    }

    /** Opens the session that {@link #write} uses. */
    void openSession() {
        int known = results.size();
        propose(leader(), new Command.RegisterSession());
        ticks(3);
        session = results.tailMap((long) known, false).values().stream().map(r -> r[0])
                .filter(r -> r instanceof SessionRegistered).map(r -> ((SessionRegistered) r).sessionId())
                .reduce((a, c) -> c).orElseThrow();
        sequence = 0;
    }

    /** Has the leader write {@code count} values of about {@code size} bytes. */
    void write(int count, int size, Random random) {
        if (session == 0) {
            openSession();
        }
        for (int i = 0; i < count; i++) {
            byte[] value = new byte[size / 2 + random.nextInt(size)];
            random.nextBytes(value);
            propose(leader(), new Command.Write(session, ++sequence,
                    List.of(new Put(b("key-" + random.nextInt(50)), value))));
            settle();
        }
        ticks(2);
    }

    /** Every running node has applied the same entries and holds the same state. */
    void assertSameState() throws IOException {
        Member reference = null;
        for (Member member : members) {
            if (!member.running()) {
                continue;
            }
            if (reference == null) {
                reference = member;
            }
            assertEquals(reference.applied(), member.applied(), "applied index of node " + member.id);
            assertEquals(reference.digest(), member.digest(), "state of node " + member.id);
        }
    }

    /** Stops every node and checks that no file handle is left open. */
    void close() throws IOException {
        for (Member member : members) {
            if (member.running()) {
                member.stop();
            }
            assertEquals(0, member.memory.openHandleCount(), "open handles on node " + member.id);
        }
    }
}
