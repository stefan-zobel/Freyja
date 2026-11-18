package io.github.stefan_zobel.freyja.node.sim;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import io.github.stefan_zobel.freyja.common.Checksums;
import io.github.stefan_zobel.freyja.common.DecodeException;
import io.github.stefan_zobel.freyja.common.LittleEndian;
import io.github.stefan_zobel.freyja.common.kv.Command;
import io.github.stefan_zobel.freyja.common.kv.Command.Condition;
import io.github.stefan_zobel.freyja.common.kv.Command.Delete;
import io.github.stefan_zobel.freyja.common.kv.Command.Mutation;
import io.github.stefan_zobel.freyja.common.kv.Command.Put;
import io.github.stefan_zobel.freyja.common.kv.CommandCodec;
import io.github.stefan_zobel.freyja.common.kv.CommandResult;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.ConditionalApplied;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.Failed;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.Failure;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.SessionRegistered;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.Written;
import io.github.stefan_zobel.freyja.lsm.EngineOptions;
import io.github.stefan_zobel.freyja.lsm.fs.FaultyFileSystem;
import io.github.stefan_zobel.freyja.lsm.fs.MemoryFileSystem;
import io.github.stefan_zobel.freyja.node.Node;
import io.github.stefan_zobel.freyja.node.NodeListener;
import io.github.stefan_zobel.freyja.node.sm.KvStateMachine;
import io.github.stefan_zobel.freyja.node.sm.StateMachineStore;
import io.github.stefan_zobel.freyja.node.snapshot.SnapshotTransfers;
import io.github.stefan_zobel.freyja.node.store.NodeIdentity;
import io.github.stefan_zobel.freyja.node.wire.MessageCodec;
import io.github.stefan_zobel.freyja.raft.ClusterConfig;
import io.github.stefan_zobel.freyja.raft.EntryType;
import io.github.stefan_zobel.freyja.raft.LogEntry;
import io.github.stefan_zobel.freyja.raft.Notification;
import io.github.stefan_zobel.freyja.raft.RaftConfig;
import io.github.stefan_zobel.freyja.raft.RaftEvent;
import io.github.stefan_zobel.freyja.raft.Role;
import io.github.stefan_zobel.freyja.raft.SnapshotMeta;
import io.github.stefan_zobel.freyja.raft.sim.LinearizabilityChecker;

/**
 * The whole-system simulation (docs/spec/verification.md, section 3): complete nodes and
 * clients in one process, on a simulated clock, a simulated network, and in-memory file
 * systems. Everything that happens follows from the seed.
 *
 * <p>Nodes exchange only bytes. Clients talk to a node as the layer that will receive
 * requests does: they hand it proposals and requests for a read index, and take the answers
 * from what the node reports. A client keeps a session and sends a command again, with the
 * same sequence number, until it is answered.
 *
 * <p>After every step the invariants of {@link SystemInvariants} are checked. At the end the
 * network heals, every node is started, and the run must converge: all nodes in the same
 * state, nothing acknowledged missing, and the history of the clients linearizable.
 */
public final class SystemSimulation {

    /**
     * What a run does.
     *
     * @param nodes       the number of nodes; all are voters from the start
     * @param clients     the number of clients
     * @param keys        the number of keys the clients use
     * @param duration    how long faults are injected and clients start operations, in units
     *                    of simulated time; a tick of a node is {@value #TICK} units
     * @param maxDelay    a message is on its way for 1 to this many units
     * @param drop        the probability that a message is lost
     * @param duplicate   the probability that a message arrives twice
     * @param chaos       the probability, each {@value #CHAOS_PERIOD} units, that a node
     *                    crashes or the network is cut
     */
    public record Options(int nodes, int clients, int keys, long duration, int maxDelay, double drop,
                          double duplicate, double chaos) {

        /** A reliable network and no crashes. */
        public static Options calm() {
            return new Options(3, 3, 4, 30_000, 3, 0, 0, 0);
        }

        /** Lost and repeated messages, partitions, and crashes, also in the middle of a write. */
        public static Options faulty() {
            return new Options(3, 4, 4, 60_000, 12, 0.03, 0.03, 0.25);
        }
    }

    /** What happened in a run; a set of runs must reach every situation. */
    public static final class Stats {
        public long steps;
        public long acknowledged;
        public long reads;
        public long casHeld;
        public long casFailed;
        public long retries;
        public long givenUp;
        public long sessionsLost;
        public long sessionsLostInRetry;
        public long lateDuplicates;
        public long leaderChanges;
        public long crashes;
        public long crashesInAnOperation;
        public long partitions;
        public long dropped;
        public long duplicated;
        public long snapshotsCreated;
        public long snapshotsInstalled;
        public long highestApplied;

        public void add(Stats other) {
            steps += other.steps;
            acknowledged += other.acknowledged;
            reads += other.reads;
            casHeld += other.casHeld;
            casFailed += other.casFailed;
            retries += other.retries;
            givenUp += other.givenUp;
            sessionsLost += other.sessionsLost;
            sessionsLostInRetry += other.sessionsLostInRetry;
            lateDuplicates += other.lateDuplicates;
            leaderChanges += other.leaderChanges;
            crashes += other.crashes;
            crashesInAnOperation += other.crashesInAnOperation;
            partitions += other.partitions;
            dropped += other.dropped;
            duplicated += other.duplicated;
            snapshotsCreated += other.snapshotsCreated;
            snapshotsInstalled += other.snapshotsInstalled;
            highestApplied += other.highestApplied;
        }

        @Override
        public String toString() {
            return "steps " + steps + ", acknowledged " + acknowledged + ", reads " + reads + ", cas held/failed "
                    + casHeld + "/" + casFailed + ", retries " + retries + ", given up " + givenUp
                    + ", sessions lost " + sessionsLost + " (" + sessionsLostInRetry
                    + " under retries), late duplicates " + lateDuplicates + ", leader changes "
                    + leaderChanges + ", crashes "
                    + crashes + " (" + crashesInAnOperation + " in an operation), partitions " + partitions
                    + ", dropped " + dropped + ", duplicated " + duplicated + ", snapshots created/installed "
                    + snapshotsCreated + "/" + snapshotsInstalled + ", applied " + highestApplied;
        }
    }

    static final Path DATA = Path.of("data");
    static final int TICK = 10;
    static final int CHAOS_PERIOD = 100;
    /** How long a client waits for an answer before it sends its request again. */
    private static final int PATIENCE = 40 * TICK;
    /** After this many requests without an answer a client gives an operation up. */
    private static final int MAX_ATTEMPTS = 12;
    /** Clients stop when this many operations have an unknown outcome: the checker pays for each. */
    private static final int MAX_GIVEN_UP = 8;
    /** Shorter than the time after which a client gives up, so that sessions expire under retries. */
    private static final long SESSION_TIMEOUT = 10L * PATIENCE;

    private static final int EVENT_TICK = 0;
    private static final int EVENT_DELIVER = 1;
    private static final int EVENT_CLIENT = 2;
    private static final int EVENT_CHAOS = 3;
    private static final int EVENT_RESTART = 4;
    private static final int EVENT_HEAL = 5;
    private static final int EVENT_REPLAY = 6;

    private record Event(long time, long order, int kind, int target, int from, byte[] frame)
            implements Comparable<Event> {
        @Override
        public int compareTo(Event other) {
            int byTime = Long.compare(time, other.time);
            return byTime != 0 ? byTime : Long.compare(order, other.order);
        }
    }

    /** A read that has its read index and waits for the node to have applied that far. */
    private record PendingRead(long requestId, long index) {
    }

    /** One node with everything that survives a crash, and what does not. */
    private final class Host implements NodeListener {
        final int id;
        final MemoryFileSystem memory = new MemoryFileSystem();
        FaultyFileSystem fs;
        Node node;
        final List<byte[]> outbox = new ArrayList<>();
        /** Proposals the core has taken, by the index it gave them: request id and term. */
        final TreeMap<Long, long[]> accepted = new TreeMap<>();
        final List<PendingRead> reads = new ArrayList<>();
        long digestedAt = -1;
        boolean crashArmed;
        Role role = Role.FOLLOWER;

        Host(int id, ClusterConfig config) throws IOException {
            this.id = id;
            Node.initialize(memory, DATA, new NodeIdentity(0x5EED, id), config, 1);
        }

        void start() throws IOException {
            fs = new FaultyFileSystem(memory);
            crashArmed = false;
            role = Role.FOLLOWER;
            digestedAt = -1;
            node = Node.open(fs, DATA, nodeOptions, new StateMachineStore.Options(new EngineOptions()
                    .deterministic(true).reserveSize(0).minFreeSpace(0).memTableSize(32 * 1024)
                    .blockCacheCapacity(1024 * 1024), SESSION_TIMEOUT), (to, frame) -> outbox.add(frame), this);
        }

        boolean running() {
            return node != null;
        }

        /** The process is gone. What it held in memory is lost with it. */
        void down(boolean crashFirst) {
            Node dead = node;
            node = null;
            stats.snapshotsCreated += dead.snapshotsCreated();
            accepted.clear();
            reads.clear();
            outbox.clear();
            if (crashFirst) {
                memory.crash(random);
            }
            try {
                dead.close(); // only releases the memory of the dead instance
            } catch (IOException | RuntimeException | FaultyFileSystem.SimulatedCrash expected) {
                // its files are gone
            }
            schedule(time + 5L * TICK + random.nextInt(60 * TICK), EVENT_RESTART, id, -1, null);
        }

        @Override
        public void notification(Notification notification) {
            switch (notification) {
                case Notification.ProposalAccepted a ->
                        accepted.put(a.index(), new long[] {a.requestId(), a.term()});
                case Notification.ProposalRejected r -> rejected(r.requestId(), r.leaderHint());
                case Notification.ReadIndexReady r -> reads.add(new PendingRead(r.requestId(), r.index()));
                case Notification.ReadIndexRejected r -> rejected(r.requestId(), r.leaderHint());
                case Notification.RoleChanged change -> {
                    if (change.role() == Role.LEADER && role != Role.LEADER) {
                        stats.leaderChanges++;
                    }
                    role = change.role();
                }
                default -> {
                }
            }
        }

        @Override
        public void applied(LogEntry entry, CommandResult result) {
            long session = 0;
            long sequence = 0;
            if (entry.type() == EntryType.COMMAND) {
                try {
                    Command command = CommandCodec.decode(entry.payload()).command();
                    if (command instanceof Command.Write write) {
                        session = write.sessionId();
                        sequence = write.sequence();
                    } else if (command instanceof Command.Conditional conditional) {
                        session = conditional.sessionId();
                        sequence = conditional.sequence();
                    }
                } catch (DecodeException e) {
                    throw new AssertionError("a command of the simulation does not decode", e);
                }
            }
            long hash = hash(entry);
            observation.applied(new SystemObservation.Applied(id, entry.index(), entry.term(), hash, session,
                    sequence, result));
            long[] proposal = accepted.remove(entry.index());
            if (proposal != null && proposal[1] == entry.term()) {
                answered(proposal[0], result, entry.index(), hash, session, sequence);
            }
        }

        @Override
        public void proposalExpired(long requestId) {
            rejected(requestId, -1);
        }

        @Override
        public void snapshotInstalled(SnapshotMeta meta) {
            stats.snapshotsInstalled++;
            // Proposals this node took may lie below the snapshot; their answers never come.
            accepted.headMap(meta.lastIncludedIndex(), true).clear();
        }
    }

    private enum Kind {
        REGISTER, WRITE, CAS, READ
    }

    /** A client with one operation at a time. */
    private final class Client {
        final int id;
        long session;
        long sequence;
        Kind kind;
        int key;
        long a;
        long b;
        long invoked;
        boolean waiting;
        /** The request ids of the attempts of the operation in hand. */
        final Set<Long> attempts = new HashSet<>();
        /** How often the client waited for an answer in vain. */
        int timeouts;
        /** Whether a node has taken a request of this operation: it may be applied. */
        boolean taken;
        /** Whether the last request was refused at once; the next turn sends it again. */
        boolean refused;
        long sentAt;
        int target = -1;
        boolean stopped;
        /** The encoded command of the operation in hand. */
        byte[] sent;

        Client(int id) {
            this.id = id;
        }

        void act() {
            if (stopped) {
                return;
            }
            if (!waiting) {
                if (!clientsMayStart) {
                    stopped = true;
                    return;
                }
                attempts.clear();
                timeouts = 0;
                taken = false;
                refused = false;
                if (session == 0) {
                    kind = Kind.REGISTER;
                } else {
                    int choice = random.nextInt(10);
                    kind = choice < 4 ? Kind.READ : choice < 8 ? Kind.WRITE : Kind.CAS;
                    key = random.nextInt(options.keys());
                    a = random.nextInt(4);
                    b = 1 + random.nextInt(3);
                    if (kind != Kind.READ) {
                        sequence++;
                    }
                }
                invoked = ++logicalTime;
                waiting = true;
                send();
            } else if (refused) {
                // Refused at once, for example by a node that is not the leader. That is
                // no sign of trouble and does not count toward giving up.
                refused = false;
                send();
            } else if (time - sentAt >= PATIENCE) {
                timeouts++;
                if (timeouts >= MAX_ATTEMPTS && kind != Kind.REGISTER) {
                    giveUp();
                } else {
                    stats.retries++;
                    target = -1;
                    send();
                }
            }
        }

        private void send() {
            sentAt = time;
            if (target < 0 || !hosts.get(target).running()) {
                target = random.nextInt(hosts.size());
            }
            Host host = hosts.get(target);
            if (!host.running()) {
                target = -1;
                refused = true; // connection refused
                return;
            }
            long requestId = ++requests;
            attempts.add(requestId);
            clientOf.put(requestId, this);
            if (kind == Kind.READ) {
                host.node.submit(new RaftEvent.ClientReadIndex(requestId));
            } else {
                Command command = switch (kind) {
                    case REGISTER -> new Command.RegisterSession();
                    case WRITE -> new Command.Write(session, sequence, List.of(mutation(key, a)));
                    default -> new Command.Conditional(session, sequence,
                            List.of(a == 0 ? Condition.notExists(key(key)) : Condition.valueEquals(key(key), value(a))),
                            List.of(mutation(key, b)), List.of());
                };
                // The receiving node stamps the command with its clock.
                sent = CommandCodec.encode(time, command);
                Node.Admission admission = host.node.propose(requestId, sent,
                        host.node.now() + 3L * PATIENCE / TICK);
                if (admission != Node.Admission.ACCEPTED) {
                    refused = true;
                } else {
                    taken = true;
                }
            }
            pump(host);
        }

        void rejected(long requestId, int leaderHint) {
            if (waiting && attempts.contains(requestId)) {
                target = leaderHint >= 0 && leaderHint < hosts.size() ? leaderHint : -1;
                refused = true;
            }
        }

        void answered(long requestId, CommandResult result, long index, long hash, long session, long sequence) {
            if (!waiting || !attempts.contains(requestId)) {
                return; // the answer to an operation that is over
            }
            if (kind != Kind.REGISTER) {
                stats.acknowledged++;
                observation.acknowledged(new SystemObservation.Acknowledged(id, session, sequence, index, hash,
                        result));
            }
            switch (result) {
                case SessionRegistered registered -> {
                    this.session = registered.sessionId();
                    this.sequence = 0;
                    waiting = false;
                }
                case Written _ -> complete(0);
                case ConditionalApplied applied -> {
                    if (applied.conditionsHeld()) {
                        stats.casHeld++;
                    } else {
                        stats.casFailed++;
                    }
                    complete(applied.conditionsHeld() ? 1 : 0);
                }
                case Failed failed when failed.failure() == Failure.NO_SUCH_SESSION -> {
                    // The session has expired. If the client waited in vain before, an
                    // earlier request may have taken effect before that: the outcome is
                    // not known. Otherwise this was the only request and it did nothing.
                    stats.sessionsLost++;
                    this.session = 0;
                    if (timeouts > 0) {
                        stats.sessionsLostInRetry++;
                        unknown();
                    } else {
                        waiting = false;
                    }
                }
                default -> throw new AssertionError("client " + id + " got " + result + " for " + kind
                        + " " + this.sequence + " of session " + this.session);
            }
        }

        void readAnswered(long requestId, long value) {
            if (waiting && kind == Kind.READ && attempts.contains(requestId)) {
                stats.reads++;
                complete(value);
            }
        }

        private void complete(long result) {
            history.add(new LinearizabilityChecker.Operation(key, switch (kind) {
                case WRITE -> LinearizabilityChecker.Kind.WRITE;
                case CAS -> LinearizabilityChecker.Kind.COMPARE_AND_SET;
                default -> LinearizabilityChecker.Kind.READ;
            }, a, b, invoked, ++logicalTime, result));
            waiting = false;
            if (kind != Kind.READ && faults && random.nextInt(4) == 0) {
                // A copy of the request that was held up somewhere arrives much later, when
                // the client has long moved on.
                stats.lateDuplicates++;
                schedule(time + random.nextInt(60 * TICK), EVENT_REPLAY, random.nextInt(hosts.size()), -1, sent);
            }
        }

        private void giveUp() {
            if (kind == Kind.READ || !taken) {
                // A read that never returned says nothing, and neither does a command that
                // no node ever took.
                waiting = false;
                return;
            }
            unknown();
        }

        /** The operation may have taken effect or not, now or later. */
        private void unknown() {
            history.add(new LinearizabilityChecker.Operation(key,
                    kind == Kind.WRITE ? LinearizabilityChecker.Kind.WRITE : LinearizabilityChecker.Kind.COMPARE_AND_SET,
                    a, b, invoked, LinearizabilityChecker.NO_RESPONSE, 0));
            waiting = false;
            stats.givenUp++;
            if (stats.givenUp >= MAX_GIVEN_UP) {
                clientsMayStart = false;
            }
        }
    }

    private final long seed;
    private final Options options;
    private final Random random;
    private final Node.Options nodeOptions;
    private final List<Host> hosts = new ArrayList<>();
    private final List<Client> clients = new ArrayList<>();
    private final PriorityQueue<Event> events = new PriorityQueue<>();
    private final TreeMap<Long, Client> clientOf = new TreeMap<>();
    private final Set<Integer> isolated = new HashSet<>();
    private final SystemObservation observation = new SystemObservation();
    private final List<LinearizabilityChecker.Operation> history = new ArrayList<>();
    private final Stats stats = new Stats();
    private long time;
    private long order;
    private long logicalTime;
    private long requests;
    private boolean clientsMayStart = true;
    private boolean faults = true;

    public SystemSimulation(long seed, Options options) {
        this.seed = seed;
        this.options = options;
        this.random = new Random(seed);
        this.nodeOptions = new Node.Options(RaftConfig.defaults(0, seed), 8 * 1024, 4096, 32 * 1024, 20,
                new SnapshotTransfers.Options(2048, 5, 60, 2),
                new Node.Limits(10_000, 1L << 40, 0, 10, Long.MAX_VALUE), () -> time);
    }

    // ------------------------------------------------------------------ helpers

    private static byte[] key(int key) {
        return ("k" + key).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] value(long value) {
        byte[] bytes = new byte[8];
        LittleEndian.putUint64(bytes, 0, value);
        return bytes;
    }

    /** The value 0 stands for a key that is not there. */
    private static Mutation mutation(int key, long value) {
        return value == 0 ? new Delete(key(key)) : new Put(key(key), value(value));
    }

    private static long hash(LogEntry entry) {
        return Checksums.crc32c(entry.payload(), 0, entry.payload().length) * 31L + entry.type().ordinal();
    }

    private void schedule(long at, int kind, int target, int from, byte[] frame) {
        events.add(new Event(at, order++, kind, target, from, frame));
    }

    private void rejected(long requestId, int leaderHint) {
        Client client = clientOf.get(requestId);
        if (client != null) {
            client.rejected(requestId, leaderHint);
        }
    }

    private void answered(long requestId, CommandResult result, long index, long hash, long session, long sequence) {
        Client client = clientOf.remove(requestId);
        if (client != null) {
            client.answered(requestId, result, index, hash, session, sequence);
        }
    }

    // ------------------------------------------------------------------ nodes and network

    /** Lets a node work until it is idle, and sends what it produced. */
    private void pump(Host host) {
        if (!host.running()) {
            return;
        }
        try {
            while (host.node.runOnce()) {
                // work the queue off
            }
            long applied = host.node.store().appliedIndex();
            if (applied != host.digestedAt) {
                host.digestedAt = applied;
                observation.digest(new SystemObservation.Digest(host.id, applied,
                        host.node.store().machine().digest()));
            }
            for (Iterator<PendingRead> it = host.reads.iterator(); it.hasNext();) {
                PendingRead read = it.next();
                if (read.index() <= applied) {
                    it.remove();
                    Client client = clientOf.remove(read.requestId());
                    if (client != null && client.waiting && client.kind == Kind.READ) {
                        KvStateMachine.Versioned stored = host.node.store().machine().get(key(client.key));
                        client.readAnswered(read.requestId(),
                                stored == null ? 0 : LittleEndian.getUint64(stored.value(), 0));
                    }
                }
            }
        } catch (FaultyFileSystem.SimulatedCrash crash) {
            stats.crashes++;
            stats.crashesInAnOperation++;
            host.down(false);
            return;
        } catch (IOException e) {
            throw new AssertionError("node " + host.id + " stopped: " + e, e);
        }
        List<byte[]> frames = new ArrayList<>(host.outbox);
        host.outbox.clear();
        for (byte[] frame : frames) {
            route(host.id, frame);
        }
    }

    private void route(int from, byte[] frame) {
        int to = MessageCodec.addressee(frame);
        if (to < 0 || to >= hosts.size()) {
            return;
        }
        if (isolated.contains(from) || isolated.contains(to) || (faults && random.nextDouble() < options.drop())) {
            stats.dropped++;
            return;
        }
        schedule(time + 1 + random.nextInt(options.maxDelay()), EVENT_DELIVER, to, from, frame);
        if (faults && random.nextDouble() < options.duplicate()) {
            stats.duplicated++;
            schedule(time + 1 + random.nextInt(options.maxDelay() * 3), EVENT_DELIVER, to, from, frame.clone());
        }
    }

    private void chaos() {
        if (random.nextDouble() >= options.chaos()) {
            return;
        }
        long down = hosts.stream().filter(h -> !h.running()).count();
        int choice = random.nextInt(10);
        if (choice < 5) {
            // Mostly a minority is down at one time, so that the cluster makes progress.
            Host victim = hosts.get(random.nextInt(hosts.size()));
            if (victim.running() && (down < (hosts.size() - 1) / 2 || random.nextInt(8) == 0)) {
                if (random.nextBoolean()) {
                    stats.crashes++;
                    victim.down(true);
                } else if (!victim.crashArmed) {
                    // The crash comes in the middle of one of the next file operations.
                    victim.crashArmed = true;
                    victim.fs.crashAt(victim.fs.operationCount() + random.nextInt(60), random,
                            () -> victim.memory.crash(random));
                }
            }
        } else if (isolated.isEmpty()) {
            stats.partitions++;
            isolated.add(random.nextInt(hosts.size()));
            schedule(time + 20L * TICK + random.nextInt(80 * TICK), EVENT_HEAL, -1, -1, null);
        }
    }

    private void handle(Event event) {
        switch (event.kind()) {
            case EVENT_TICK -> {
                Host host = hosts.get(event.target());
                if (host.running()) {
                    host.node.tick();
                    pump(host);
                }
                schedule(time + TICK, EVENT_TICK, host.id, -1, null);
            }
            case EVENT_DELIVER -> {
                Host host = hosts.get(event.target());
                if (host.running() && !isolated.contains(host.id) && !isolated.contains(event.from())) {
                    host.node.offer(event.frame());
                    pump(host);
                }
            }
            case EVENT_CLIENT -> {
                Client client = clients.get(event.target());
                client.act();
                if (!client.stopped) {
                    schedule(time + TICK + random.nextInt(3 * TICK), EVENT_CLIENT, client.id, -1, null);
                }
            }
            case EVENT_CHAOS -> {
                if (faults) {
                    chaos();
                    schedule(time + CHAOS_PERIOD, EVENT_CHAOS, -1, -1, null);
                }
            }
            case EVENT_RESTART -> {
                Host host = hosts.get(event.target());
                if (!host.running()) {
                    try {
                        host.start();
                    } catch (IOException e) {
                        throw new AssertionError("node " + host.id + " does not start: " + e, e);
                    }
                    pump(host);
                }
            }
            case EVENT_HEAL -> isolated.clear();
            case EVENT_REPLAY -> {
                Host host = hosts.get(event.target());
                if (host.running()) {
                    // Nobody waits for the answer.
                    host.node.propose(++requests, event.frame(), host.node.now() + 3L * PATIENCE / TICK);
                    pump(host);
                }
            }
            default -> throw new IllegalStateException("event " + event.kind());
        }
    }

    private void step() {
        Event event = events.poll();
        time = Math.max(time, event.time());
        observation.beginStep();
        handle(event);
        SystemInvariants.checkAll(observation);
        stats.steps++;
    }

    // ------------------------------------------------------------------ the run

    /**
     * Runs the simulation.
     *
     * @return what happened
     * @throws AssertionError if an invariant does not hold, the run does not converge, or the
     *                        history of the clients is not linearizable; the message names
     *                        the seed
     */
    public Stats run() {
        try {
            simulate();
            return stats;
        } catch (AssertionError | RuntimeException | IOException e) {
            throw new AssertionError("whole-system simulation, seed " + seed + ", time " + time + ": "
                    + e.getMessage() + " [" + stats + "]", e);
        } finally {
            for (Host host : hosts) {
                if (host.running()) {
                    try {
                        host.node.close();
                    } catch (IOException | RuntimeException | FaultyFileSystem.SimulatedCrash ignored) {
                        // the run is over
                    }
                    host.node = null;
                }
            }
        }
    }

    private void simulate() throws IOException {
        Integer[] ids = new Integer[options.nodes()];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = i;
        }
        ClusterConfig config = ClusterConfig.ofVoters(ids);
        for (int i = 0; i < options.nodes(); i++) {
            Host host = new Host(i, config);
            hosts.add(host);
            host.start();
            schedule(random.nextInt(TICK), EVENT_TICK, i, -1, null);
        }
        for (int i = 0; i < options.clients(); i++) {
            clients.add(new Client(i));
            schedule(30L * TICK + random.nextInt(10 * TICK), EVENT_CLIENT, i, -1, null);
        }
        schedule(CHAOS_PERIOD, EVENT_CHAOS, -1, -1, null);

        while (time < options.duration()) {
            step();
        }

        // Healing: no more faults, every node runs, clients finish what they have in hand.
        faults = false;
        clientsMayStart = false;
        isolated.clear();
        for (Host host : hosts) {
            if (host.running() && host.crashArmed) {
                host.fs.crashAt(-1, null, null);
                host.crashArmed = false;
            }
        }
        long deadline = time + 4_000L * TICK;
        while (!converged()) {
            if (time > deadline) {
                throw new AssertionError("the cluster does not converge after healing: " + describe());
            }
            step();
        }
        finish();
    }

    private boolean converged() throws IOException {
        long applied = -1;
        for (Host host : hosts) {
            if (!host.running() || host.node.view().logRecovering()) {
                return false;
            }
            long own = host.node.store().appliedIndex();
            if (applied != -1 && own != applied) {
                return false;
            }
            applied = own;
        }
        if (applied < observation.highestApplied()) {
            return false;
        }
        for (Client client : clients) {
            if (client.waiting && !client.stopped) {
                return false;
            }
        }
        return true;
    }

    private String describe() {
        StringBuilder text = new StringBuilder();
        for (Host host : hosts) {
            text.append("node ").append(host.id).append(host.running()
                    ? " " + host.node.view() + " applied " + host.node.store().appliedIndex() : " down").append("; ");
        }
        for (Client client : clients) {
            text.append("client ").append(client.id).append(client.waiting ? " waits for " + client.kind : " idle")
                    .append("; ");
        }
        return text.toString();
    }

    private void finish() throws IOException {
        long digest = 0;
        long applied = 0;
        for (Host host : hosts) {
            long own = host.node.store().machine().digest();
            if (host.id > 0 && own != digest) {
                throw new AssertionError("after healing node " + host.id + " is in another state than node 0");
            }
            digest = own;
            applied = host.node.store().appliedIndex();
            stats.snapshotsCreated += host.node.snapshotsCreated();
        }
        for (SystemObservation.Acknowledged ack : observation.acknowledged()) {
            if (ack.index() > applied) {
                throw new AssertionError("the acknowledged entry " + ack.index() + " is beyond what the nodes"
                        + " have applied after healing: " + applied);
            }
        }
        stats.highestApplied = applied;
        String violation = LinearizabilityChecker.check(history);
        if (violation != null) {
            throw new AssertionError("the history is not linearizable: " + violation);
        }
        for (Host host : hosts) {
            host.node.close();
            host.node = null;
            if (host.memory.openHandleCount() != 0) {
                throw new AssertionError("node " + host.id + " left " + host.memory.openHandleCount()
                        + " file handles open");
            }
        }
    }

    /** For tests of the simulation itself: the clients' history. */
    List<LinearizabilityChecker.Operation> history() {
        return history;
    }

    @Override
    public String toString() {
        return "SystemSimulation(seed " + seed + ", " + options + ", " + Arrays.toString(new long[] {time}) + ")";
    }
}
