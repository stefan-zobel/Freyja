package io.github.sz.freyja.node.sm;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.CRC32C;

import io.github.sz.freyja.common.ByteComparator;
import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.LittleEndian;
import io.github.sz.freyja.common.kv.Command;
import io.github.sz.freyja.common.kv.Command.Alarm;
import io.github.sz.freyja.common.kv.Command.ClearAlarm;
import io.github.sz.freyja.common.kv.Command.CloseSession;
import io.github.sz.freyja.common.kv.Command.Condition;
import io.github.sz.freyja.common.kv.Command.Conditional;
import io.github.sz.freyja.common.kv.Command.Delete;
import io.github.sz.freyja.common.kv.Command.KeepAlive;
import io.github.sz.freyja.common.kv.Command.Mutation;
import io.github.sz.freyja.common.kv.Command.Put;
import io.github.sz.freyja.common.kv.Command.RegisterSession;
import io.github.sz.freyja.common.kv.Command.SetAddress;
import io.github.sz.freyja.common.kv.Command.SetAlarm;
import io.github.sz.freyja.common.kv.Command.SetClusterVersion;
import io.github.sz.freyja.common.kv.Command.Write;
import io.github.sz.freyja.common.kv.CommandCodec;
import io.github.sz.freyja.common.kv.CommandResult;
import io.github.sz.freyja.common.kv.CommandResult.AdminApplied;
import io.github.sz.freyja.common.kv.CommandResult.ConditionalApplied;
import io.github.sz.freyja.common.kv.CommandResult.Failed;
import io.github.sz.freyja.common.kv.CommandResult.Failure;
import io.github.sz.freyja.common.kv.CommandResult.SessionOk;
import io.github.sz.freyja.common.kv.CommandResult.SessionRegistered;
import io.github.sz.freyja.common.kv.CommandResult.Written;
import io.github.sz.freyja.lsm.EngineOptions;
import io.github.sz.freyja.lsm.LsmEngine;
import io.github.sz.freyja.lsm.ScanIterator;
import io.github.sz.freyja.lsm.WriteBatch;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.node.StateMachine;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.EntryType;
import io.github.sz.freyja.raft.LogEntry;

/**
 * The replicated key-value state machine on a storage engine (docs/spec/node.md, section 4).
 *
 * <p>Every key of the engine starts with one byte that names its space: 0x00 is system state
 * (the applied index, the configuration, the sessions), 0x01 is user data. A stored user
 * value starts with the log index of the command that wrote it (its mod index).
 *
 * <p>A batch of committed entries is applied in one write of the engine, together with the
 * applied index and the sessions it touched. The engine is not synced: the Raft log is the
 * durable record. After a crash the engine holds a prefix of what was applied, the applied
 * index tells which, and the node applies the rest of the log again.
 *
 * <p>The state is a function of the applied entries alone: time is the largest time stamp of
 * an applied command, never a clock of this node.
 *
 * <p>An instance is confined to one thread.
 */
public final class KvStateMachine implements StateMachine, Closeable {

    /** A value with the log index of the command that wrote it. */
    public record Versioned(byte[] value, long modIndex) {
    }

    /** One pair of a scan. */
    public record Pair(byte[] key, byte[] value, long modIndex) {
    }

    private static final byte SYSTEM = 0x00;
    private static final byte USER = 0x01;
    private static final byte[] META_KEY = {SYSTEM, 'm'};
    private static final byte[] CONFIG_KEY = {SYSTEM, 'c'};
    private static final byte[] ALARM_KEY = {SYSTEM, 'a'};
    private static final byte[] VERSION_KEY = {SYSTEM, 'v'};
    private static final byte SESSION = 's';
    private static final byte ADDRESS = 'n';

    /** Where a node listens: for peers and for clients, each as {@code host:port}. */
    public record NodeAddress(String peer, String client) {
    }

    /** What the state machine remembers of a session. */
    private static final class Session {
        long lastActivity;
        long lastSequence;
        /** The encoded result of the command with {@code lastSequence}; empty if there is none. */
        byte[] lastResult = new byte[0];
    }

    private final LsmEngine engine;
    private final long sessionTimeoutMillis;
    private long appliedIndex;
    private long appliedTerm;
    /** State-machine time: the largest time stamp applied so far. */
    private long time;
    private long lastSweep;
    private ClusterConfig config;
    private long configIndex;
    private final TreeMap<Long, Session> sessions = new TreeMap<>();
    private final EnumSet<Alarm> alarms = EnumSet.noneOf(Alarm.class);
    private final TreeMap<Integer, NodeAddress> addresses = new TreeMap<>();
    private long addressChanges;
    private int clusterVersion;

    private KvStateMachine(LsmEngine engine, long sessionTimeoutMillis, ClusterConfig initialConfig,
                           int initialClusterVersion) {
        this.engine = engine;
        this.sessionTimeoutMillis = sessionTimeoutMillis;
        this.config = initialConfig;
        this.clusterVersion = initialClusterVersion;
    }

    /**
     * Opens the state machine in {@code directory}.
     *
     * @param sessionTimeoutMillis a session expires when state-machine time exceeds its last
     *                             activity by this much; it must be the same on all nodes
     * @param initialConfig        the configuration the cluster started with; it counts until
     *                             a CONFIG entry is applied
     * @param initialClusterVersion the cluster version the cluster started with; it counts
     *                             until the version is raised through the log
     */
    public static KvStateMachine open(StorageFileSystem fs, Path directory, EngineOptions options,
                                      long sessionTimeoutMillis, ClusterConfig initialConfig,
                                      int initialClusterVersion) throws IOException {
        LsmEngine engine = LsmEngine.open(fs, directory, options);
        boolean opened = false;
        try {
            KvStateMachine machine = new KvStateMachine(engine, sessionTimeoutMillis, initialConfig,
                    initialClusterVersion);
            machine.load();
            opened = true;
            return machine;
        } finally {
            if (!opened) {
                engine.close();
            }
        }
    }

    private void load() throws IOException {
        byte[] meta = engine.get(META_KEY);
        if (meta != null) {
            appliedIndex = LittleEndian.getUint64(meta, 0);
            appliedTerm = LittleEndian.getUint64(meta, 8);
            time = LittleEndian.getUint64(meta, 16);
            lastSweep = LittleEndian.getUint64(meta, 24);
        }
        byte[] stored = engine.get(CONFIG_KEY);
        if (stored != null) {
            configIndex = LittleEndian.getUint64(stored, 0);
            config = ClusterConfig.decode(Arrays.copyOfRange(stored, 8, stored.length));
        }
        byte[] storedAlarms = engine.get(ALARM_KEY);
        if (storedAlarms != null) {
            long bits = LittleEndian.getUint64(storedAlarms, 0);
            for (Alarm alarm : Alarm.values()) {
                if ((bits & 1L << alarm.ordinal()) != 0) {
                    alarms.add(alarm);
                }
            }
        }
        byte[] storedVersion = engine.get(VERSION_KEY);
        if (storedVersion != null) {
            clusterVersion = LittleEndian.getInt32(storedVersion, 0);
        }
        try (ScanIterator scan = engine.scan(new byte[] {SYSTEM, ADDRESS}, new byte[] {SYSTEM, ADDRESS + 1})) {
            for (; scan.valid(); scan.next()) {
                byte[] value = scan.value();
                int peerLength = LittleEndian.getInt32(value, 0);
                addresses.put(LittleEndian.getInt32(scan.key(), 2), new NodeAddress(
                        new String(value, 4, peerLength, java.nio.charset.StandardCharsets.UTF_8),
                        new String(value, 4 + peerLength, value.length - 4 - peerLength,
                                java.nio.charset.StandardCharsets.UTF_8)));
            }
        }
        try (ScanIterator scan = engine.scan(new byte[] {SYSTEM, SESSION}, new byte[] {SYSTEM, SESSION + 1})) {
            for (; scan.valid(); scan.next()) {
                Session session = new Session();
                byte[] value = scan.value();
                session.lastActivity = LittleEndian.getUint64(value, 0);
                session.lastSequence = LittleEndian.getUint64(value, 8);
                session.lastResult = Arrays.copyOfRange(value, 16, value.length);
                sessions.put(LittleEndian.getUint64(scan.key(), 2), session);
            }
        }
    }

    // ------------------------------------------------------------------ status

    @Override
    public long appliedIndex() {
        return appliedIndex;
    }

    /** The term of the last entry applied. */
    public long appliedTerm() {
        return appliedTerm;
    }

    /** State-machine time in milliseconds. */
    public long time() {
        return time;
    }

    /** The configuration as of the applied index. */
    public ClusterConfig config() {
        return config;
    }

    /** The index of the entry that carried {@link #config}; 0 for the initial configuration. */
    public long configIndex() {
        return configIndex;
    }

    /** The alarms that are set, as of the applied index. */
    public Set<Alarm> alarms() {
        return Collections.unmodifiableSet(alarms);
    }

    /** Where the nodes listen, by node id, as of the applied index. */
    public Map<Integer, NodeAddress> addresses() {
        return Collections.unmodifiableMap(addresses);
    }

    /** Counts the changes of {@link #addresses} since this state machine was opened. */
    public long addressChanges() {
        return addressChanges;
    }

    /** The cluster version as of the applied index. */
    public int clusterVersion() {
        return clusterVersion;
    }

    /** The number of open sessions. */
    public int sessionCount() {
        return sessions.size();
    }

    // ------------------------------------------------------------------ apply

    /** What a batch has changed so far, for the commands that follow in the same batch. */
    private static final class Pending {
        final WriteBatch batch = new WriteBatch();
        /** User keys written in this batch; an empty array stands for a deleted key. */
        final TreeMap<byte[], byte[]> written = new TreeMap<>(ByteComparator.UNSIGNED);
        final TreeSet<byte[]> deleted = new TreeSet<>(ByteComparator.UNSIGNED);
        final TreeSet<Long> touchedSessions = new TreeSet<>();
    }

    @Override
    public List<CommandResult> apply(List<LogEntry> entries) throws IOException {
        List<CommandResult> results = new ArrayList<>(entries.size());
        if (entries.isEmpty()) {
            return results;
        }
        Pending pending = new Pending();
        for (LogEntry entry : entries) {
            if (entry.index() != appliedIndex + 1) {
                throw new IllegalArgumentException("entry " + entry + " does not follow the applied index "
                        + appliedIndex);
            }
            CommandResult result = null;
            if (entry.type() == EntryType.CONFIG) {
                config = ClusterConfig.decode(entry.payload());
                configIndex = entry.index();
                byte[] encoded = config.encode();
                byte[] stored = new byte[8 + encoded.length];
                LittleEndian.putUint64(stored, 0, configIndex);
                System.arraycopy(encoded, 0, stored, 8, encoded.length);
                pending.batch.put(CONFIG_KEY, stored);
            } else if (entry.type() == EntryType.COMMAND) {
                result = applyCommand(entry, pending);
            }
            results.add(result);
            appliedIndex = entry.index();
            appliedTerm = entry.term();
            sweepSessions(pending);
        }
        for (long id : pending.touchedSessions) {
            Session session = sessions.get(id);
            if (session == null) {
                pending.batch.delete(sessionKey(id));
            } else {
                byte[] value = new byte[16 + session.lastResult.length];
                LittleEndian.putUint64(value, 0, session.lastActivity);
                LittleEndian.putUint64(value, 8, session.lastSequence);
                System.arraycopy(session.lastResult, 0, value, 16, session.lastResult.length);
                pending.batch.put(sessionKey(id), value);
            }
        }
        byte[] meta = new byte[32];
        LittleEndian.putUint64(meta, 0, appliedIndex);
        LittleEndian.putUint64(meta, 8, appliedTerm);
        LittleEndian.putUint64(meta, 16, time);
        LittleEndian.putUint64(meta, 24, lastSweep);
        pending.batch.put(META_KEY, meta);
        // Not synced: the Raft log is the durable record.
        engine.write(pending.batch, false);
        return results;
    }

    private CommandResult applyCommand(LogEntry entry, Pending pending) throws IOException {
        CommandCodec.Stamped stamped;
        try {
            if (entry.payload().length > Command.MAX_REQUEST_SIZE) {
                return new Failed(Failure.INVALID);
            }
            stamped = CommandCodec.decode(entry.payload());
        } catch (DecodeException e) {
            // Every node finds the same bytes invalid, so every node gives the same answer.
            return new Failed(Failure.INVALID);
        }
        time = Math.max(time, stamped.timestampMillis());
        return switch (stamped.command()) {
            case RegisterSession _ -> {
                Session session = new Session();
                session.lastActivity = time;
                sessions.put(entry.index(), session);
                pending.touchedSessions.add(entry.index());
                yield new SessionRegistered(entry.index());
            }
            case KeepAlive keepAlive -> {
                Session session = liveSession(keepAlive.sessionId(), pending);
                if (session == null) {
                    yield new Failed(Failure.NO_SUCH_SESSION);
                }
                session.lastActivity = time;
                pending.touchedSessions.add(keepAlive.sessionId());
                yield new SessionOk();
            }
            case CloseSession close -> {
                if (liveSession(close.sessionId(), pending) == null) {
                    yield new Failed(Failure.NO_SUCH_SESSION);
                }
                sessions.remove(close.sessionId());
                pending.touchedSessions.add(close.sessionId());
                yield new SessionOk();
            }
            case SetAlarm set -> {
                if (alarms.add(set.alarm())) {
                    storeAlarms(pending);
                }
                yield new AdminApplied();
            }
            case ClearAlarm clear -> {
                if (alarms.remove(clear.alarm())) {
                    storeAlarms(pending);
                }
                yield new AdminApplied();
            }
            case SetClusterVersion set -> {
                if (set.version() < clusterVersion) {
                    yield new Failed(Failure.VERSION_TOO_OLD);
                }
                if (set.version() > clusterVersion) {
                    clusterVersion = set.version();
                    byte[] stored = new byte[4];
                    LittleEndian.putInt32(stored, 0, clusterVersion);
                    pending.batch.put(VERSION_KEY, stored);
                }
                yield new AdminApplied();
            }
            case SetAddress set -> {
                NodeAddress address = new NodeAddress(set.peerAddress(), set.clientAddress());
                if (!address.equals(addresses.put(set.nodeId(), address))) {
                    addressChanges++;
                    byte[] peer = set.peerAddress().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    byte[] client = set.clientAddress().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    byte[] stored = new byte[4 + peer.length + client.length];
                    LittleEndian.putInt32(stored, 0, peer.length);
                    System.arraycopy(peer, 0, stored, 4, peer.length);
                    System.arraycopy(client, 0, stored, 4 + peer.length, client.length);
                    byte[] key = new byte[6];
                    key[0] = SYSTEM;
                    key[1] = ADDRESS;
                    LittleEndian.putInt32(key, 2, set.nodeId());
                    pending.batch.put(key, stored);
                }
                yield new AdminApplied();
            }
            case Write write -> inSession(write.sessionId(), write.sequence(), pending, () -> {
                if (!valid(write.mutations())) {
                    return new Failed(Failure.INVALID);
                }
                if (alarms.contains(Alarm.NOSPACE) && puts(write.mutations())) {
                    return new Failed(Failure.NO_SPACE);
                }
                write(write.mutations(), entry.index(), pending);
                return new Written();
            });
            case Conditional conditional -> inSession(conditional.sessionId(), conditional.sequence(),
                    pending, () -> {
                        if (!valid(conditional.then()) || !valid(conditional.otherwise())
                                || !validConditions(conditional.conditions())) {
                            return new Failed(Failure.INVALID);
                        }
                        // Decided without looking at the conditions: what a client may send
                        // while the alarm is set does not depend on the data.
                        if (alarms.contains(Alarm.NOSPACE)
                                && (puts(conditional.then()) || puts(conditional.otherwise()))) {
                            return new Failed(Failure.NO_SPACE);
                        }
                        boolean held = true;
                        for (Condition condition : conditional.conditions()) {
                            held &= holds(condition, pending);
                        }
                        write(held ? conditional.then() : conditional.otherwise(), entry.index(), pending);
                        return new ConditionalApplied(held);
                    });
        };
    }

    /** A step of a command that may read the engine. */
    @FunctionalInterface
    private interface Effect {
        CommandResult run() throws IOException;
    }

    /**
     * Exactly-once: a command is applied if its sequence number is above the last one of its
     * session. The command with the last sequence number is answered from the record.
     */
    private CommandResult inSession(long sessionId, long sequence, Pending pending, Effect effect)
            throws IOException {
        Session session = liveSession(sessionId, pending);
        if (session == null) {
            return new Failed(Failure.NO_SUCH_SESSION);
        }
        session.lastActivity = time;
        pending.touchedSessions.add(sessionId);
        if (sequence < session.lastSequence) {
            return new Failed(Failure.SEQUENCE_TOO_OLD);
        }
        if (sequence == session.lastSequence && session.lastResult.length > 0) {
            return CommandCodec.decodeResult(session.lastResult);
        }
        CommandResult result = effect.run();
        if (result instanceof Failed(Failure failure) && failure == Failure.NO_SPACE) {
            // Not recorded: the client may send the command again when the alarm is cleared.
            return result;
        }
        session.lastSequence = sequence;
        session.lastResult = CommandCodec.encode(result);
        return result;
    }

    /** The session if it is open and has not expired; an expired one is closed here. */
    private Session liveSession(long id, Pending pending) {
        Session session = sessions.get(id);
        if (session != null && time - session.lastActivity > sessionTimeoutMillis) {
            sessions.remove(id);
            pending.touchedSessions.add(id);
            return null;
        }
        return session;
    }

    /**
     * Closes the sessions that have expired, a few times per timeout of state-machine time.
     * When it runs depends only on what was applied, so it runs alike on every node.
     */
    private void sweepSessions(Pending pending) {
        if (time - lastSweep < Math.max(1, sessionTimeoutMillis / 4)) {
            return;
        }
        lastSweep = time;
        for (Iterator<Map.Entry<Long, Session>> it = sessions.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Long, Session> entry = it.next();
            if (time - entry.getValue().lastActivity > sessionTimeoutMillis) {
                pending.touchedSessions.add(entry.getKey());
                it.remove();
            }
        }
    }

    private static boolean valid(List<Mutation> mutations) {
        for (Mutation mutation : mutations) {
            if (mutation.key().length == 0 || mutation.key().length > Command.MAX_KEY_SIZE
                    || (mutation instanceof Put put && put.value().length > Command.MAX_VALUE_SIZE)) {
                return false;
            }
        }
        return true;
    }

    private static boolean puts(List<Mutation> mutations) {
        for (Mutation mutation : mutations) {
            if (mutation instanceof Put) {
                return true;
            }
        }
        return false;
    }

    private void storeAlarms(Pending pending) {
        long bits = 0;
        for (Alarm alarm : alarms) {
            bits |= 1L << alarm.ordinal();
        }
        byte[] stored = new byte[8];
        LittleEndian.putUint64(stored, 0, bits);
        pending.batch.put(ALARM_KEY, stored);
    }

    private static boolean validConditions(List<Condition> conditions) {
        for (Condition condition : conditions) {
            if (condition.key().length == 0 || condition.key().length > Command.MAX_KEY_SIZE
                    || condition.value().length > Command.MAX_VALUE_SIZE) {
                return false;
            }
        }
        return true;
    }

    private void write(List<Mutation> mutations, long index, Pending pending) {
        for (Mutation mutation : mutations) {
            byte[] key = userKey(mutation.key());
            switch (mutation) {
                case Put put -> {
                    byte[] stored = new byte[8 + put.value().length];
                    LittleEndian.putUint64(stored, 0, index);
                    System.arraycopy(put.value(), 0, stored, 8, put.value().length);
                    pending.batch.put(key, stored);
                    pending.written.put(key, stored);
                    pending.deleted.remove(key);
                }
                case Delete _ -> {
                    pending.batch.delete(key);
                    pending.written.remove(key);
                    pending.deleted.add(key);
                }
            }
        }
    }

    private boolean holds(Condition condition, Pending pending) throws IOException {
        byte[] key = userKey(condition.key());
        byte[] stored = pending.deleted.contains(key) ? null
                : pending.written.containsKey(key) ? pending.written.get(key) : engine.get(key);
        return switch (condition.kind()) {
            case EXISTS -> stored != null;
            case NOT_EXISTS -> stored == null;
            case VALUE_EQUALS -> stored != null
                    && Arrays.equals(stored, 8, stored.length, condition.value(), 0, condition.value().length);
            case MOD_INDEX_EQUALS -> stored != null && LittleEndian.getUint64(stored, 0) == condition.modIndex();
        };
    }

    // ------------------------------------------------------------------ reads

    /** The value of a user key as of the applied index, or {@code null}. */
    public Versioned get(byte[] key) throws IOException {
        byte[] stored = engine.get(userKey(key));
        return stored == null ? null
                : new Versioned(Arrays.copyOfRange(stored, 8, stored.length), LittleEndian.getUint64(stored, 0));
    }

    /**
     * The user keys from {@code start} (inclusive) to {@code end} (exclusive) in ascending
     * order, at most {@code limit} of them.
     *
     * @param start {@code null} for the first key
     * @param end   {@code null} for no upper bound
     */
    public List<Pair> scan(byte[] start, byte[] end, int limit) throws IOException {
        return scan(start, end, limit, false);
    }

    /**
     * The user keys of the range from {@code start} (inclusive) to {@code end} (exclusive), at
     * most {@code limit} of them: the smallest ones in ascending order, or, for a reverse
     * scan, the largest ones in descending order.
     *
     * @param start {@code null} for the first key
     * @param end   {@code null} for no upper bound
     */
    public List<Pair> scan(byte[] start, byte[] end, int limit, boolean reverse) throws IOException {
        List<Pair> pairs = new ArrayList<>();
        byte[] from = start == null ? new byte[] {USER} : userKey(start);
        byte[] to = end == null ? new byte[] {USER + 1} : userKey(end);
        try (ScanIterator scan = reverse ? engine.scanReverse(from, to) : engine.scan(from, to)) {
            for (; scan.valid() && pairs.size() < limit; scan.next()) {
                byte[] key = scan.key();
                byte[] stored = scan.value();
                pairs.add(new Pair(Arrays.copyOfRange(key, 1, key.length),
                        Arrays.copyOfRange(stored, 8, stored.length), LittleEndian.getUint64(stored, 0)));
            }
        }
        return pairs;
    }

    /**
     * A checksum over the complete state, system and user. Two state machines that have
     * applied the same entries have the same digest.
     */
    public long digest() throws IOException {
        CRC32C crc = new CRC32C();
        byte[] length = new byte[8];
        try (ScanIterator scan = engine.scan(null, null)) {
            for (; scan.valid(); scan.next()) {
                for (byte[] part : new byte[][] {scan.key(), scan.value()}) {
                    LittleEndian.putUint64(length, 0, part.length);
                    crc.update(length, 0, 8);
                    crc.update(part, 0, part.length);
                }
            }
        }
        return crc.getValue();
    }

    /** The engine, for checkpoints and for tests. */
    public LsmEngine engine() {
        return engine;
    }

    @Override
    public void close() throws IOException {
        engine.close();
    }

    private static byte[] userKey(byte[] key) {
        byte[] full = new byte[1 + key.length];
        full[0] = USER;
        System.arraycopy(key, 0, full, 1, key.length);
        return full;
    }

    private static byte[] sessionKey(long id) {
        byte[] key = new byte[10];
        key[0] = SYSTEM;
        key[1] = SESSION;
        LittleEndian.putUint64(key, 2, id);
        return key;
    }
}
