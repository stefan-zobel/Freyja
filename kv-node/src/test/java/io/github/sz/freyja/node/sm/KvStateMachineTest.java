package io.github.sz.freyja.node.sm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.common.kv.Command;
import io.github.sz.freyja.common.kv.Command.Condition;
import io.github.sz.freyja.common.kv.Command.Delete;
import io.github.sz.freyja.common.kv.Command.Mutation;
import io.github.sz.freyja.common.kv.Command.Put;
import io.github.sz.freyja.common.kv.CommandCodec;
import io.github.sz.freyja.common.kv.CommandResult;
import io.github.sz.freyja.common.kv.CommandResult.AdminApplied;
import io.github.sz.freyja.common.kv.CommandResult.ConditionalApplied;
import io.github.sz.freyja.common.kv.CommandResult.Failed;
import io.github.sz.freyja.common.kv.CommandResult.Failure;
import io.github.sz.freyja.common.kv.CommandResult.SessionOk;
import io.github.sz.freyja.common.kv.CommandResult.SessionRegistered;
import io.github.sz.freyja.common.kv.CommandResult.Written;
import io.github.sz.freyja.common.testing.Seeds;
import io.github.sz.freyja.lsm.EngineOptions;
import io.github.sz.freyja.lsm.fs.MemoryFileSystem;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.ConfigChange;
import io.github.sz.freyja.raft.EntryType;
import io.github.sz.freyja.raft.LogEntry;

/** The key-value state machine (docs/spec/node.md, section 4). */
class KvStateMachineTest {

    private static final Path DIR = Path.of("data", "sm");
    private static final ClusterConfig THREE = ClusterConfig.ofVoters(0, 1, 2);
    private static final long TIMEOUT = 60_000;

    private static EngineOptions engineOptions() {
        return new EngineOptions().deterministic(true).reserveSize(0).minFreeSpace(0)
                .memTableSize(16 * 1024).blockCacheCapacity(1024 * 1024).level0CompactionTrigger(2);
    }

    private static KvStateMachine open(MemoryFileSystem fs) throws IOException {
        fs.createDirectories(DIR.getParent());
        return KvStateMachine.open(fs, DIR, engineOptions(), TIMEOUT, THREE, 1);
    }

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String s(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Builds the log: every command becomes the next entry. */
    private static final class Log {
        final List<LogEntry> entries = new ArrayList<>();
        long time = 1_000;

        LogEntry command(Command command) {
            LogEntry entry = new LogEntry(entries.size() + 1, 1, EntryType.COMMAND,
                    CommandCodec.encode(time, command));
            entries.add(entry);
            return entry;
        }

        LogEntry other(EntryType type, byte[] payload) {
            LogEntry entry = new LogEntry(entries.size() + 1, 1, type, payload);
            entries.add(entry);
            return entry;
        }
    }

    private static CommandResult apply(KvStateMachine machine, LogEntry entry) throws IOException {
        return machine.apply(List.of(entry)).getFirst();
    }

    @Test
    void writesReadsAndScansCarryTheIndexOfTheWritingCommand() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        Log log = new Log();
        try (KvStateMachine machine = open(fs)) {
            long session = ((SessionRegistered) apply(machine, log.command(new Command.RegisterSession()))).sessionId();
            assertEquals(1, session, "the session id is the index of the registration");
            assertEquals(new Written(), apply(machine, log.command(new Command.Write(session, 1,
                    List.of(new Put(b("b"), b("two")), new Put(b("a"), b("one")), new Put(b("c"), new byte[0]))))));
            assertEquals(new Written(), apply(machine, log.command(new Command.Write(session, 2,
                    List.of(new Delete(b("b")), new Put(b("d"), b("four")))))));
            assertEquals(3, machine.appliedIndex());

            assertEquals("one", s(machine.get(b("a")).value()));
            assertEquals(2, machine.get(b("a")).modIndex());
            assertNull(machine.get(b("b")));
            assertEquals(0, machine.get(b("c")).value().length);
            assertEquals(3, machine.get(b("d")).modIndex());

            List<KvStateMachine.Pair> all = machine.scan(null, null, 100);
            assertEquals(List.of("a", "c", "d"), all.stream().map(p -> s(p.key())).toList());
            assertEquals(List.of("c"), machine.scan(b("b"), b("d"), 100).stream().map(p -> s(p.key())).toList());
            assertEquals(2, machine.scan(null, null, 2).size());
            // Backward: system keys below and above the user keys stay out of it.
            assertEquals(List.of("d", "c", "a"),
                    machine.scan(null, null, 100, true).stream().map(p -> s(p.key())).toList());
            assertEquals(List.of("d", "c"), machine.scan(null, null, 2, true).stream().map(p -> s(p.key())).toList());
            assertEquals(List.of("c", "a"),
                    machine.scan(b("a"), b("d"), 100, true).stream().map(p -> s(p.key())).toList());
            assertEquals(3, machine.scan(null, null, 1, true).getFirst().modIndex());
            assertEquals(3, all.getLast().modIndex());
        }
        try (KvStateMachine machine = open(fs)) {
            assertEquals(3, machine.appliedIndex());
            assertEquals(1, machine.appliedTerm());
            assertEquals("four", s(machine.get(b("d")).value()));
            assertEquals(1, machine.sessionCount());
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void aConditionalBatchAppliesOneOfItsTwoBatches() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        Log log = new Log();
        try (KvStateMachine machine = open(fs)) {
            long session = ((SessionRegistered) apply(machine, log.command(new Command.RegisterSession()))).sessionId();
            apply(machine, log.command(new Command.Write(session, 1, List.of(new Put(b("k"), b("v1"))))));

            // All four kinds of conditions hold.
            assertEquals(new ConditionalApplied(true), apply(machine, log.command(new Command.Conditional(session, 2,
                    List.of(Condition.exists(b("k")), Condition.notExists(b("other")),
                            Condition.valueEquals(b("k"), b("v1")), Condition.modIndexEquals(b("k"), 2)),
                    List.of(new Put(b("k"), b("v2"))), List.of(new Put(b("failed"), b("x")))))));
            assertEquals("v2", s(machine.get(b("k")).value()));
            assertEquals(3, machine.get(b("k")).modIndex());
            assertNull(machine.get(b("failed")));

            // One condition does not hold: the other batch is applied.
            assertEquals(new ConditionalApplied(false), apply(machine, log.command(new Command.Conditional(session, 3,
                    List.of(Condition.exists(b("k")), Condition.modIndexEquals(b("k"), 2)),
                    List.of(new Put(b("k"), b("v3"))), List.of(new Put(b("failed"), b("x")))))));
            assertEquals("v2", s(machine.get(b("k")).value()));
            assertEquals("x", s(machine.get(b("failed")).value()));

            // Commands of one batch see what the commands before them in the batch did.
            List<CommandResult> results = machine.apply(List.of(
                    log.command(new Command.Write(session, 4, List.of(new Put(b("n"), b("1")), new Delete(b("k"))))),
                    log.command(new Command.Conditional(session, 5,
                            List.of(Condition.valueEquals(b("n"), b("1")), Condition.notExists(b("k"))),
                            List.of(new Put(b("n"), b("2"))), List.of())),
                    log.command(new Command.Conditional(session, 6,
                            List.of(Condition.valueEquals(b("n"), b("1"))), List.of(new Put(b("n"), b("3"))),
                            List.of()))));
            assertEquals(List.of(new Written(), new ConditionalApplied(true), new ConditionalApplied(false)), results);
            assertEquals("2", s(machine.get(b("n")).value()));
        }
    }

    @Test
    void aCommandIsAppliedOnceHoweverOftenItIsInTheLog() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        Log log = new Log();
        try (KvStateMachine machine = open(fs)) {
            long session = ((SessionRegistered) apply(machine, log.command(new Command.RegisterSession()))).sessionId();
            Command create = new Command.Conditional(session, 1, List.of(Condition.notExists(b("k"))),
                    List.of(new Put(b("k"), b("first"))), List.of());
            assertEquals(new ConditionalApplied(true), apply(machine, log.command(create)));
            // The client did not get the answer and sent the command again: the same answer,
            // although the condition would not hold a second time.
            assertEquals(new ConditionalApplied(true), apply(machine, log.command(create)));
            assertEquals(2, machine.get(b("k")).modIndex(), "written once");

            apply(machine, log.command(new Command.Write(session, 2, List.of(new Put(b("k"), b("second"))))));
            assertEquals(new Failed(Failure.SEQUENCE_TOO_OLD), apply(machine, log.command(create)),
                    "the answer to an older command is no longer kept");
            assertEquals("second", s(machine.get(b("k")).value()));

            assertEquals(new Failed(Failure.NO_SUCH_SESSION),
                    apply(machine, log.command(new Command.Write(999, 1, List.of(new Put(b("x"), b("y")))))));
            assertNull(machine.get(b("x")));

            assertEquals(new SessionOk(), apply(machine, log.command(new Command.CloseSession(session))));
            assertEquals(new Failed(Failure.NO_SUCH_SESSION),
                    apply(machine, log.command(new Command.Write(session, 3, List.of(new Put(b("x"), b("y")))))));
            assertEquals(new Failed(Failure.NO_SUCH_SESSION), apply(machine, log.command(new Command.CloseSession(session))));
            assertEquals(0, machine.sessionCount());
        }
        // The record of the session survives a restart.
        Log second = new Log();
        MemoryFileSystem other = new MemoryFileSystem();
        long session;
        try (KvStateMachine machine = open(other)) {
            session = ((SessionRegistered) apply(machine, second.command(new Command.RegisterSession()))).sessionId();
            apply(machine, second.command(new Command.Write(session, 5, List.of(new Put(b("k"), b("v"))))));
        }
        try (KvStateMachine machine = open(other)) {
            assertEquals(new Written(), apply(machine, second.command(
                    new Command.Write(session, 5, List.of(new Put(b("k"), b("again")))))));
            assertEquals("v", s(machine.get(b("k")).value()), "answered from the record, not applied");
        }
    }

    @Test
    void aSessionExpiresByTheTimeOfTheStateMachine() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        Log log = new Log();
        try (KvStateMachine machine = open(fs)) {
            long idle = ((SessionRegistered) apply(machine, log.command(new Command.RegisterSession()))).sessionId();
            long active = ((SessionRegistered) apply(machine, log.command(new Command.RegisterSession()))).sessionId();
            assertEquals(1_000, machine.time());

            log.time += TIMEOUT - 1;
            assertEquals(new SessionOk(), apply(machine, log.command(new Command.KeepAlive(active))));
            // A command with an older time stamp does not turn time back.
            log.time -= 5_000;
            apply(machine, log.command(new Command.KeepAlive(active)));
            assertEquals(1_000 + TIMEOUT - 1, machine.time());

            log.time += 5_002;
            apply(machine, log.command(new Command.KeepAlive(active)));
            // The sweep ran a moment ago and is not due yet: the session is found expired
            // when it is used.
            assertEquals(2, machine.sessionCount());
            assertEquals(new Failed(Failure.NO_SUCH_SESSION),
                    apply(machine, log.command(new Command.Write(idle, 1, List.of(new Put(b("k"), b("v")))))));
            assertEquals(1, machine.sessionCount());
            assertEquals(new Written(),
                    apply(machine, log.command(new Command.Write(active, 1, List.of(new Put(b("k"), b("v")))))));

            // Without any activity for longer than the timeout, the next use finds it expired.
            log.time += TIMEOUT + 1;
            assertEquals(new Failed(Failure.NO_SUCH_SESSION), apply(machine, log.command(new Command.KeepAlive(active))));
            assertEquals(0, machine.sessionCount());
        }
        try (KvStateMachine machine = open(fs)) {
            assertEquals(0, machine.sessionCount());
            assertEquals(log.time, machine.time());
        }
    }

    @Test
    void entriesWithoutACommandAndInvalidCommandsAreAppliedAlikeEverywhere() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        Log log = new Log();
        ClusterConfig grown = THREE.with(new ConfigChange.AddLearner(5));
        try (KvStateMachine machine = open(fs)) {
            assertEquals(THREE, machine.config());
            List<CommandResult> results = machine.apply(List.of(
                    log.other(EntryType.NOOP, new byte[0]),
                    log.other(EntryType.CONFIG, grown.encode()),
                    log.other(EntryType.COMMAND, new byte[] {1, 2, 3}),
                    log.command(new Command.RegisterSession())));
            assertNull(results.get(0));
            assertNull(results.get(1));
            assertEquals(new Failed(Failure.INVALID), results.get(2), "bytes that are no command");
            long session = ((SessionRegistered) results.get(3)).sessionId();
            assertEquals(4, machine.appliedIndex());
            assertEquals(grown, machine.config());
            assertEquals(2, machine.configIndex());

            assertEquals(new Failed(Failure.INVALID), apply(machine, log.command(
                    new Command.Write(session, 1, List.of(new Put(new byte[0], b("v")))))), "an empty key");
            assertEquals(new Failed(Failure.INVALID), apply(machine, log.command(new Command.Write(session, 2,
                    List.of(new Put(b("k"), b("v")), new Put(new byte[Command.MAX_KEY_SIZE + 1], b("v")))))));
            assertNull(machine.get(b("k")), "nothing of an invalid batch is applied");
            assertThrows(IllegalArgumentException.class,
                    () -> machine.apply(List.of(new LogEntry(99, 1, EntryType.NOOP, new byte[0]))));
        }
        try (KvStateMachine machine = open(fs)) {
            assertEquals(grown, machine.config());
            assertEquals(2, machine.configIndex());
        }
    }

    @Test
    void whileTheSpaceAlarmIsSetOnlyDeletesAreAccepted() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        Log log = new Log();
        try (KvStateMachine machine = open(fs)) {
            long session = ((SessionRegistered) apply(machine, log.command(new Command.RegisterSession()))).sessionId();
            apply(machine, log.command(new Command.Write(session, 1, List.of(new Put(b("k"), b("v"))))));
            assertTrue(machine.alarms().isEmpty());

            assertEquals(new AdminApplied(), apply(machine, log.command(new Command.SetAlarm(Command.Alarm.NOSPACE))));
            assertEquals(new AdminApplied(), apply(machine, log.command(new Command.SetAlarm(Command.Alarm.NOSPACE))));
            assertEquals(Set.of(Command.Alarm.NOSPACE), machine.alarms());

            Command put = new Command.Write(session, 2, List.of(new Put(b("more"), b("data"))));
            assertEquals(new Failed(Failure.NO_SPACE), apply(machine, log.command(put)));
            assertEquals(new Failed(Failure.NO_SPACE), apply(machine, log.command(new Command.Conditional(session, 2,
                    List.of(Condition.exists(b("k"))), List.of(new Delete(b("k"))),
                    List.of(new Put(b("x"), b("y")))))), "a put in either batch");
            assertNull(machine.get(b("more")));
            assertEquals("v", s(machine.get(b("k")).value()));
            // Sessions and deletes still work.
            assertEquals(new SessionOk(), apply(machine, log.command(new Command.KeepAlive(session))));
            assertEquals(new ConditionalApplied(true), apply(machine, log.command(new Command.Conditional(session, 2,
                    List.of(Condition.exists(b("k"))), List.of(new Delete(b("k"))), List.of()))));
            assertNull(machine.get(b("k")));
        }
        try (KvStateMachine machine = open(fs)) {
            assertEquals(Set.of(Command.Alarm.NOSPACE), machine.alarms(), "the alarm survives a restart");
            long session = 1;
            assertEquals(new AdminApplied(), apply(machine, log.command(new Command.ClearAlarm(Command.Alarm.NOSPACE))));
            assertEquals(new AdminApplied(), apply(machine, log.command(new Command.ClearAlarm(Command.Alarm.NOSPACE))));
            assertTrue(machine.alarms().isEmpty());
            // The rejected command did not use up its sequence number.
            assertEquals(new Written(), apply(machine, log.command(
                    new Command.Write(session, 3, List.of(new Put(b("more"), b("data")))))));
            assertEquals("data", s(machine.get(b("more")).value()));
        }
    }

    @Test
    void theAddressesOfTheNodesAreInTheReplicatedState() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        Log log = new Log();
        try (KvStateMachine machine = open(fs)) {
            assertTrue(machine.addresses().isEmpty());
            assertEquals(new AdminApplied(), apply(machine, log.command(new Command.SetAddress(2, "h2:7000", "h2:7001"))));
            assertEquals(new AdminApplied(), apply(machine, log.command(new Command.SetAddress(0, "h0:7000", "h0:7001"))));
            assertEquals(2, machine.addressChanges());
            apply(machine, log.command(new Command.SetAddress(2, "h2:7000", "h2:7001")));
            assertEquals(2, machine.addressChanges(), "the same address again changes nothing");
            apply(machine, log.command(new Command.SetAddress(2, "other:7000", "")));
            assertEquals(3, machine.addressChanges());
            assertEquals(new KvStateMachine.NodeAddress("other:7000", ""), machine.addresses().get(2));
        }
        try (KvStateMachine machine = open(fs)) {
            assertEquals(Set.of(0, 2), machine.addresses().keySet());
            assertEquals(new KvStateMachine.NodeAddress("h0:7000", "h0:7001"), machine.addresses().get(0));
            assertEquals(new KvStateMachine.NodeAddress("other:7000", ""), machine.addresses().get(2));
        }
    }

    @Test
    void theClusterVersionOnlyRises() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        Log log = new Log();
        try (KvStateMachine machine = open(fs)) {
            assertEquals(1, machine.clusterVersion(), "the version the cluster was created with");
            assertEquals(new AdminApplied(), apply(machine, log.command(new Command.SetClusterVersion(1))));
            assertEquals(new AdminApplied(), apply(machine, log.command(new Command.SetClusterVersion(3))));
            assertEquals(new AdminApplied(), apply(machine, log.command(new Command.SetClusterVersion(3))));
            assertEquals(new Failed(Failure.VERSION_TOO_OLD),
                    apply(machine, log.command(new Command.SetClusterVersion(2))));
            assertEquals(3, machine.clusterVersion());
        }
        try (KvStateMachine machine = open(fs)) {
            assertEquals(3, machine.clusterVersion());
        }
    }

    /** A random command of a client that has the session {@code session}. */
    private static Command randomCommand(Random random, long session, long sequence) {
        List<Mutation> mutations = new ArrayList<>();
        int count = 1 + random.nextInt(3);
        for (int i = 0; i < count; i++) {
            byte[] key = b("key-" + random.nextInt(40));
            if (random.nextInt(4) == 0) {
                mutations.add(new Delete(key));
            } else {
                byte[] value = new byte[random.nextInt(300)];
                random.nextBytes(value);
                mutations.add(new Put(key, value));
            }
        }
        if (random.nextInt(3) == 0) {
            byte[] key = b("key-" + random.nextInt(40));
            Condition condition = switch (random.nextInt(3)) {
                case 0 -> Condition.exists(key);
                case 1 -> Condition.notExists(key);
                default -> Condition.modIndexEquals(key, 1 + random.nextInt(50));
            };
            return new Command.Conditional(session, sequence, List.of(condition), mutations,
                    List.of(new Put(b("else"), b("" + sequence))));
        }
        return new Command.Write(session, sequence, mutations);
    }

    @Test
    void afterCrashesAndReplaysTheStateIsThatOfApplyingTheLogOnce() {
        Seeds.forEach(KvStateMachineTest.class, "state-machine-replay", 30, seed -> {
            Random random = new Random(seed);
            // The log: sessions, commands (some sent twice), keep-alives, other entries.
            Log log = new Log();
            long session = 0;
            long sequence = 0;
            for (int i = 0; i < 400; i++) {
                log.time += random.nextInt(500);
                int choice = random.nextInt(20);
                if (session == 0 || choice == 0) {
                    session = log.command(new Command.RegisterSession()).index();
                    sequence = 0;
                } else if (choice == 1) {
                    log.other(EntryType.NOOP, new byte[0]);
                } else if (choice == 2) {
                    log.command(new Command.KeepAlive(session));
                } else if (choice == 3) {
                    log.command(random.nextBoolean() ? new Command.SetAlarm(Command.Alarm.NOSPACE)
                            : new Command.ClearAlarm(Command.Alarm.NOSPACE));
                    if (random.nextInt(4) == 0) {
                        log.command(new Command.SetClusterVersion(1 + random.nextInt(4)));
                    }
                } else {
                    Command command = randomCommand(random, session, ++sequence);
                    log.command(command);
                    if (random.nextInt(5) == 0) {
                        log.command(command); // a retry of the client
                    }
                }
            }
            // The reference: the log applied once, entry by entry, without any fault.
            long expected;
            long expectedTime;
            List<CommandResult> expectedResults = new ArrayList<>();
            try (KvStateMachine reference = open(new MemoryFileSystem())) {
                for (LogEntry entry : log.entries) {
                    expectedResults.add(apply(reference, entry));
                }
                expected = reference.digest();
                expectedTime = reference.time();
            }
            // The same log in batches of random size, with crashes; after each crash the log is
            // applied again from the index the state machine has.
            MemoryFileSystem fs = new MemoryFileSystem();
            int crashes = 0;
            long applied = 0;
            while (applied < log.entries.size()) {
                KvStateMachine machine = open(fs);
                {
                    applied = machine.appliedIndex();
                    int steps = 1 + random.nextInt(12);
                    for (int i = 0; i < steps && applied < log.entries.size(); i++) {
                        int size = 1 + random.nextInt(20);
                        List<LogEntry> batch = log.entries.subList((int) applied,
                                (int) Math.min(log.entries.size(), applied + size));
                        List<CommandResult> results = machine.apply(batch);
                        assertEquals(expectedResults.subList((int) applied, (int) applied + batch.size()), results,
                                "the results do not depend on batching or on a replay");
                        applied += batch.size();
                        if (random.nextInt(6) == 0) {
                            machine.engine().compact();
                        }
                    }
                    if (applied < log.entries.size()) {
                        // The handles of the crashed instance died with the crash; closing it
                        // only releases its memory.
                        fs.crash(random);
                        crashes++;
                        try {
                            machine.close();
                        } catch (IOException filesAreGone) {
                            // the files are gone
                        }
                    } else {
                        machine.close();
                    }
                }
            }
            try (KvStateMachine machine = open(fs)) {
                assertEquals(log.entries.size(), machine.appliedIndex());
                assertEquals(expected, machine.digest());
                assertEquals(expectedTime, machine.time());
            }
            assertTrue(crashes > 3, "crashes: " + crashes);
            assertEquals(0, fs.openHandleCount());
        });
    }
}
