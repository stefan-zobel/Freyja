package io.github.sz.freyja.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.common.kv.Command;
import io.github.sz.freyja.common.kv.Command.Condition;
import io.github.sz.freyja.common.kv.Command.Delete;
import io.github.sz.freyja.common.kv.Command.Put;
import io.github.sz.freyja.common.kv.CommandCodec;
import io.github.sz.freyja.common.kv.CommandResult;
import io.github.sz.freyja.common.kv.CommandResult.SessionRegistered;
import io.github.sz.freyja.common.testing.Seeds;
import io.github.sz.freyja.lsm.EngineOptions;
import io.github.sz.freyja.lsm.fs.MemoryFileSystem;
import io.github.sz.freyja.node.sm.KvStateMachine;
import io.github.sz.freyja.node.store.NodeIdentity;
import io.github.sz.freyja.node.wire.MessageCodec;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.LogEntry;
import io.github.sz.freyja.raft.Notification;
import io.github.sz.freyja.raft.RaftConfig;
import io.github.sz.freyja.raft.RaftEvent;
import io.github.sz.freyja.raft.Role;

/**
 * Complete nodes with the key-value state machine: what the nodes apply is the same on all
 * of them, also when nodes crash and apply parts of the log a second time.
 */
class ReplicatedStoreTest {

    private static final ClusterConfig THREE = ClusterConfig.ofVoters(0, 1, 2);
    private static final Path DATA = Path.of("data");
    private static final Path MACHINE = DATA.resolve("sm").resolve("1");

    private final class Member implements NodeListener {
        final int id;
        final MemoryFileSystem memory = new MemoryFileSystem();
        Node node;
        KvStateMachine machine;

        Member(int id) throws IOException {
            this.id = id;
            Node.initialize(memory, DATA, new NodeIdentity(7, id));
            memory.createDirectories(MACHINE.getParent());
        }

        void start() throws IOException {
            machine = KvStateMachine.open(memory, MACHINE, new EngineOptions().deterministic(true).reserveSize(0)
                    .minFreeSpace(0).memTableSize(32 * 1024).blockCacheCapacity(1024 * 1024), 60_000, THREE, 1);
            node = Node.open(memory, DATA, new Node.Options(RaftConfig.defaults(0, 5), 16 * 1024, 256), THREE,
                    machine, (to, message) -> network.add(message), this);
        }

        void crash(Random random) throws IOException {
            node.close();
            node = null;
            memory.crash(random);
            try {
                machine.close();
            } catch (IOException filesAreGone) {
                // closing only releases the memory of the crashed instance
            }
            machine = null;
        }

        void stop() throws IOException {
            node.close();
            machine.close();
        }

        @Override
        public void notification(Notification notification) {
        }

        @Override
        public void applied(LogEntry entry, CommandResult result) {
            // Every node, every time it applies an entry, gets the same result.
            CommandResult[] known = results.putIfAbsent(entry.index(), new CommandResult[] {result});
            if (known != null) {
                assertEquals(known[0], result, "entry " + entry.index() + " on node " + id);
            }
        }
    }

    private final ArrayDeque<byte[]> network = new ArrayDeque<>();
    private final List<Member> members = new ArrayList<>();
    /** The result of every applied entry, by index; an array so that null can be stored. */
    private final TreeMap<Long, CommandResult[]> results = new TreeMap<>();

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
                    member.node.tick();
                }
            }
            settle();
        }
    }

    private Member leader() throws IOException {
        for (int i = 0; i < 400; i++) {
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

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private long time = 1_000;
    private long requestId;

    private void propose(Member member, Command command) {
        member.node.submit(new RaftEvent.ClientPropose(++requestId, CommandCodec.encode(time += 10, command)));
    }

    @Test
    void allNodesHoldTheSameStateAlsoAfterCrashes() {
        Seeds.forEach(ReplicatedStoreTest.class, "replicated-store", 6, seed -> {
            network.clear();
            members.clear();
            results.clear();
            Random random = new Random(seed);
            for (int id = 0; id < 3; id++) {
                Member member = new Member(id);
                members.add(member);
                member.start();
            }
            propose(leader(), new Command.RegisterSession());
            ticks(3);
            long session = results.values().stream().map(r -> r[0]).filter(r -> r instanceof SessionRegistered)
                    .map(r -> ((SessionRegistered) r).sessionId()).findFirst().orElseThrow();
            long sequence = 0;
            int crashes = 0;
            for (int round = 0; round < 25; round++) {
                Member leader = leader();
                for (int i = 0; i < 8; i++) {
                    byte[] key = b("key-" + random.nextInt(30));
                    byte[] value = new byte[random.nextInt(400)];
                    random.nextBytes(value);
                    Command command = switch (random.nextInt(4)) {
                        case 0 -> new Command.Write(session, ++sequence, List.of(new Delete(key)));
                        case 1 -> new Command.Conditional(session, ++sequence, List.of(Condition.notExists(key)),
                                List.of(new Put(key, value)), List.of(new Delete(key)));
                        default -> new Command.Write(session, ++sequence, List.of(new Put(key, value)));
                    };
                    propose(leader, command);
                    if (random.nextInt(3) == 0) {
                        propose(leader, command); // the client sends it a second time
                    }
                    if (random.nextBoolean()) {
                        settle();
                    }
                }
                if (random.nextInt(3) == 0) {
                    Member victim = members.get(random.nextInt(3));
                    victim.crash(random);
                    crashes++;
                    ticks(random.nextInt(25));
                    victim.start();
                }
                ticks(3);
            }
            Member leader = leader();
            ticks(30);
            long digest = leader.machine.digest();
            for (Member member : members) {
                assertEquals(leader.machine.appliedIndex(), member.machine.appliedIndex());
                assertEquals(digest, member.machine.digest(), "node " + member.id);
                assertFalse(member.node.view().logRecovering());
                member.stop();
                assertEquals(0, member.memory.openHandleCount());
            }
            assertTrue(leader.machine.appliedIndex() > 100, "applied: " + leader.machine.appliedIndex());
            assertTrue(crashes > 2, "crashes: " + crashes);
        });
    }
}
