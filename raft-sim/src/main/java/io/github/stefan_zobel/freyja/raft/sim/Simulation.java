package io.github.stefan_zobel.freyja.raft.sim;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import io.github.stefan_zobel.freyja.raft.ClusterConfig;
import io.github.stefan_zobel.freyja.raft.LogEntry;
import io.github.stefan_zobel.freyja.raft.Notification;
import io.github.stefan_zobel.freyja.raft.Notification.ProposalAccepted;
import io.github.stefan_zobel.freyja.raft.Notification.ProposalRejected;
import io.github.stefan_zobel.freyja.raft.Notification.ReadIndexReady;
import io.github.stefan_zobel.freyja.raft.Notification.ReadIndexRejected;
import io.github.stefan_zobel.freyja.raft.Notification.Reason;
import io.github.stefan_zobel.freyja.raft.Notification.RoleChanged;
import io.github.stefan_zobel.freyja.raft.Outbound;
import io.github.stefan_zobel.freyja.raft.PersistedState;
import io.github.stefan_zobel.freyja.raft.ProtocolState;
import io.github.stefan_zobel.freyja.raft.RaftConfig;
import io.github.stefan_zobel.freyja.raft.RaftEvent;
import io.github.stefan_zobel.freyja.raft.RaftMessage;
import io.github.stefan_zobel.freyja.raft.RaftEvent.ClientPropose;
import io.github.stefan_zobel.freyja.raft.RaftEvent.ClientReadIndex;
import io.github.stefan_zobel.freyja.raft.RaftEvent.MessageReceived;
import io.github.stefan_zobel.freyja.raft.RaftEvent.Persisted;
import io.github.stefan_zobel.freyja.raft.RaftEvent.Tick;
import io.github.stefan_zobel.freyja.raft.RaftNode;
import io.github.stefan_zobel.freyja.raft.Ready;
import io.github.stefan_zobel.freyja.raft.Role;
import io.github.stefan_zobel.freyja.raft.SnapshotMeta;
import io.github.stefan_zobel.freyja.raft.sim.LinearizabilityChecker.Kind;
import io.github.stefan_zobel.freyja.raft.sim.LinearizabilityChecker.Operation;

/**
 * A deterministic simulation of a Raft cluster (docs/spec/verification.md, section 2.4).
 *
 * <p>Virtual time is driven by a priority queue of events. Everything random comes from the
 * seed of the options, so a run is reproducible.
 *
 * <ul>
 *   <li><b>Network:</b> messages travel for a random time and overtake each other. They may be
 *       lost or duplicated, and the network may be partitioned, in both or in one direction.</li>
 *   <li><b>Storage:</b> writes become durable in order after a random delay; one completion may
 *       cover several writes.</li>
 *   <li><b>Processes:</b> a node may pause (it processes nothing for a while, then everything
 *       that arrived in the meantime), and it may crash. A crash discards what was not
 *       durable; of the writes on their way any prefix may have reached the disk. The state
 *       machine survives a crash or is rebuilt from the log.</li>
 *   <li><b>State machine:</b> registers. Two clients run: one produces load, the other a
 *       small history of reads, writes, and conditional writes that is checked for
 *       linearizability.</li>
 * </ul>
 *
 * <p>After every step the safety invariants are checked ({@link SafetyInvariants}).
 */
public final class Simulation {

    /** Counters that tell what a run exercised. */
    public static final class Stats {
        public long events;
        public long messages;
        public long dropped;
        public long duplicated;
        public long proposals;
        public long accepted;
        public long rejectedNotLeader;
        public long rejectedOverloaded;
        /** Accepted proposals whose entry was replaced by another one or dropped. */
        public long lost;
        public long appliedCommands;
        public long truncations;
        public long writes;
        public long batchedWrites;
        public long pauses;
        public long crashes;
        public long diskStalls;
        public long restarts;
        public long partitions;
        public long checkedOperations;
        public long readsServed;
        public long readsRejected;
        /** Reads sent to the leader of before, right after a write completed under a new one. */
        public long staleLeaderReads;

        @Override
        public String toString() {
            return "events=" + events + " messages=" + messages + " dropped=" + dropped
                    + " duplicated=" + duplicated + " proposals=" + proposals + " accepted=" + accepted
                    + " notLeader=" + rejectedNotLeader + " overloaded=" + rejectedOverloaded
                    + " lost=" + lost + " appliedCommands=" + appliedCommands
                    + " truncations=" + truncations + " writes=" + writes
                    + " batchedWrites=" + batchedWrites + " pauses=" + pauses + " crashes=" + crashes
                    + " diskStalls=" + diskStalls
                    + " partitions=" + partitions + " checkedOperations=" + checkedOperations
                    + " readsServed=" + readsServed + " readsRejected=" + readsRejected
                    + " staleLeaderReads=" + staleLeaderReads;
        }
    }

    /** Is told about every step of every node, for trace recording. */
    @FunctionalInterface
    public interface StepListener {
        /**
         * @param readies the work the node handed out during the step, in order
         */
        void stepped(long time, int node, RaftEvent event, ProtocolState state, List<Ready> readies);

        /** A node started again after a crash, with the state it found on its disk. */
        default void restarted(long time, int node, ProtocolState state) {
        }
    }

    private record Scheduled(long time, long order, Runnable action) implements Comparable<Scheduled> {
        @Override
        public int compareTo(Scheduled other) {
            int byTime = Long.compare(time, other.time);
            return byTime != 0 ? byTime : Long.compare(order, other.order);
        }
    }

    private record Proposal(long index, long term) {
    }

    /** One operation of the checked history. */
    static final class ClientOperation {
        final int key;
        final Kind kind;
        final long a;
        final long b;
        final long invoke;
        /** Virtual time of the invocation. */
        long invokedAt;
        long response = LinearizabilityChecker.NO_RESPONSE;
        long result;
        /** The operation is known to have had no effect and to have returned nothing. */
        boolean discarded;
        /** Where the accepted proposal sits, or the read index; -1 until known. */
        long index = -1;
        long term = -1;

        ClientOperation(int key, Kind kind, long a, long b, long invoke) {
            this.key = key;
            this.kind = kind;
            this.a = a;
            this.b = b;
            this.invoke = invoke;
        }
    }

    /** Request ids from here on belong to checked operations. */
    private static final long CHECKED_BASE = 1L << 50;
    private static final int CHECKED_KEYS = 3;
    /** The checked client waits for answers to at most this many operations at a time. */
    private static final int CHECKED_CONCURRENCY = 4;
    /** After this time without an answer the checked client stops waiting for an operation. */
    private static final int CHECKED_PATIENCE = 2_000;
    /** The checked client stops when it has given up on this many operations. */
    private static final int CHECKED_GIVEN_UP = 8;
    private static final int COMMAND_HEADER = 26;
    private static final byte COMMAND_WRITE = 1;
    private static final byte COMMAND_COMPARE_AND_SET = 2;

    private final SimOptions options;
    private final Random random;
    private final PriorityQueue<Scheduled> queue = new PriorityQueue<>();
    private final List<SimNode> nodes = new ArrayList<>();
    private final ClusterConfig cluster;
    private final Observation observation;
    private final Stats stats = new Stats();
    private long now;
    private long order;
    private long traceHash;
    private StepListener listener;
    private final List<Ready> stepReadies = new ArrayList<>();

    // ---- faults in force
    private int minDelay;
    private int maxDelay;
    private int maxPersistDelay;
    private int dropPercent;
    private int duplicatePercent;
    private boolean faults = true;
    /** {@code blocked[from][to]}: messages from {@code from} do not reach {@code to}. */
    private final boolean[][] blocked;
    private long partitionEpoch;

    // ---- the clients
    private boolean workload = true;
    private long nextCommandId = 1;
    private int knownLeader = RaftNode.NO_NODE;
    private long knownLeaderTerm;
    /** The leader the clients knew before the current one. It may still believe it leads. */
    private int previousLeader = RaftNode.NO_NODE;
    /** Virtual time at which the clients learned of the current leader. */
    private long leaderChangedAt;
    /** For a while after a change of leader, completed writes are read back from the old one. */
    private static final int STALE_LEADER_WINDOW = 400;
    /** Accepted proposals by command id. */
    private final Map<Long, Proposal> acceptedProposals = new HashMap<>();
    private final Set<Long> appliedCommandIds = new TreeSet<>();
    private final List<ClientOperation> checked = new ArrayList<>();
    /** Checked operations the client still waits for. */
    private final List<ClientOperation> outstanding = new ArrayList<>();
    private int givenUp;
    private long historyClock;
    private final List<List<Long>> valuesWritten = new ArrayList<>();

    public Simulation(SimOptions options) {
        this.options = options;
        this.random = new Random(options.seed());
        this.minDelay = options.minDelay();
        this.maxDelay = options.maxDelay();
        this.maxPersistDelay = options.maxPersistDelay();
        this.dropPercent = options.dropPercent();
        this.duplicatePercent = options.duplicatePercent();
        this.blocked = new boolean[options.nodes()][options.nodes()];
        this.observation = new Observation(options.nodes());

        Set<Integer> ids = new TreeSet<>();
        for (int i = 0; i < options.nodes(); i++) {
            ids.add(i);
        }
        cluster = new ClusterConfig(ids, Set.of());
        for (int i = 0; i < options.nodes(); i++) {
            RaftConfig config = new RaftConfig(i, random.nextLong(), options.electionTicks(),
                    options.heartbeatTicks(), options.maxInflightAppends(), options.maxAppendBytes(),
                    options.maxUncommittedBytes());
            int spread = options.tickSpread();
            int interval = options.tickMillis() + (spread == 0 ? 0 : random.nextInt(2 * spread + 1) - spread);
            SimNode node = new SimNode(i, config, interval);
            node.raft = RaftNode.create(config, PersistedState.initial(cluster), node.store, 0);
            nodes.add(node);
            schedule(random.nextInt(interval) + 1, () -> tick(node));
        }
        for (int key = 0; key < CHECKED_KEYS; key++) {
            valuesWritten.add(new ArrayList<>(List.of(0L)));
        }
        schedule(options.proposalInterval(), this::loadTurn);
        schedule(100, this::checkedTurn);
        schedule(500, this::faultTurn);
    }

    // ------------------------------------------------------------------ control

    /** Processes every event up to {@code millis} of virtual time from now. */
    public void run(long millis) {
        long end = now + millis;
        while (!queue.isEmpty() && queue.peek().time() <= end) {
            Scheduled next = queue.poll();
            now = next.time();
            next.action().run();
        }
        now = end;
        checkComplete();
    }

    /**
     * Ends all faults: the network delivers every message once and quickly, partitions are
     * lifted, nodes that are down start again, and nothing pauses or crashes any more.
     */
    public void calm() {
        faults = false;
        minDelay = 1;
        maxDelay = 3;
        maxPersistDelay = 2;
        dropPercent = 0;
        duplicatePercent = 0;
        healPartition();
        for (SimNode node : nodes) {
            if (!node.up) {
                restart(node);
            }
        }
    }

    /** Switches the clients on or off. */
    public void setWorkload(boolean on) {
        workload = on;
    }

    /** Registers the listener that is told about every step. */
    public void setStepListener(StepListener listener) {
        this.listener = listener;
    }

    public long now() {
        return now;
    }

    public List<SimNode> nodes() {
        return nodes;
    }

    public Observation observation() {
        return observation;
    }

    public Stats stats() {
        return stats;
    }

    public SimOptions options() {
        return options;
    }

    /** A hash over every {@code Ready} of every node in the order they were produced. */
    public long traceHash() {
        return traceHash;
    }

    /**
     * Liveness after the network has been calm for a while: one leader, every node follows
     * it, and every log is complete, committed, and applied.
     *
     * @throws AssertionError if the cluster is not in that state
     */
    public void checkConverged() {
        String problem = notConverged();
        if (problem != null) {
            throw new AssertionError(problem);
        }
    }

    /** Whether the cluster is in the state {@link #checkConverged} demands. */
    public boolean isConverged() {
        return notConverged() == null;
    }

    private String notConverged() {
        SimNode leader = null;
        for (SimNode node : nodes) {
            if (!node.up) {
                return "node " + node.id + " is down";
            }
            if (node.view().role() == Role.LEADER) {
                if (leader != null) {
                    return "two leaders: " + leader.view() + " and " + node.view();
                }
                leader = node;
            }
        }
        if (leader == null) {
            return "no leader: " + nodes.stream().map(SimNode::view).toList();
        }
        long last = leader.view().lastLogIndex();
        for (SimNode node : nodes) {
            ProtocolState view = node.view();
            if (view.term() != leader.view().term() || view.leaderId() != leader.id
                    || view.lastLogIndex() != last || view.commitIndex() != last
                    || view.durableIndex() != last || node.applied != last) {
                return "node " + node.id + " has not converged: " + view + ", applied "
                        + node.applied + "; leader: " + leader.view();
            }
            if (!Arrays.equals(node.registers, leader.registers)) {
                return "the state machines of nodes " + node.id + " and " + leader.id + " differ";
            }
        }
        if (observation.appliedLog().size() != last) {
            return "applied " + observation.appliedLog().size() + ", log end " + last;
        }
        return null;
    }

    // ------------------------------------------------------------------ the runtime of a node

    private void schedule(long delay, Runnable action) {
        queue.add(new Scheduled(now + delay, order++, action));
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private void checkComplete() {
        String violation = SafetyInvariants.logMatchingComplete(observation);
        check(violation == null, "LogMatching violated: " + violation);
    }

    /** Hands an event to a node, performs the work that follows, and checks the invariants. */
    private void step(SimNode node, RaftEvent event) {
        observation.beginStep();
        stepReadies.clear();
        node.raft.step(event);
        stats.events++;
        drain(node);

        ProtocolState view = node.view();
        observation.sawCommitted(view.commitIndex(), view.term());
        SafetyInvariants.checkAll(observation);
        check(view.term() >= node.lastSeenTerm, "node " + node.id + ": the term went back: " + view);
        node.lastSeenTerm = view.term();
        check(view.durableIndex() <= view.lastLogIndex() && view.commitIndex() <= view.lastLogIndex()
                        && view.durableIndex() <= node.store.lastIndex(),
                "node " + node.id + ": inconsistent indexes " + view + ", stored " + node.store.lastIndex());
        check(view.lastLogIndex() == observation.log(node.id).size(),
                "node " + node.id + ": the log ends at " + view.lastLogIndex() + ", handed out were "
                        + observation.log(node.id).size() + " entries");
        if (stats.events % 512 == 0) {
            checkComplete();
        }
        if (listener != null) {
            listener.stepped(now, node.id, event, view, List.copyOf(stepReadies));
        }
    }

    /** Hands an event to a node, keeps it while the node pauses, drops it if the node is down. */
    private void deliver(SimNode node, RaftEvent event) {
        if (!node.up) {
            return;
        }
        if (now < node.pausedUntil) {
            schedule(node.pausedUntil - now, () -> deliver(node, event));
        } else {
            step(node, event);
        }
    }

    private void tick(SimNode node) {
        schedule(node.tickInterval, () -> tick(node));
        if (node.up && now >= node.pausedUntil) {
            step(node, new Tick()); // a paused node does not make up for missed ticks
        }
    }

    private void drain(SimNode node) {
        while (node.raft.hasReady()) {
            Ready ready = node.raft.takeReady();
            traceHash = mix(mix(traceHash, node.id), hashOf(ready));
            if (listener != null) {
                stepReadies.add(ready);
            }
            check(ready.sequence() == node.lastReadySequence + 1,
                    "Ready " + ready.sequence() + " follows " + node.lastReadySequence);
            node.lastReadySequence = ready.sequence();

            if (ready.hasPersistentWork()) {
                recordLogChange(node, ready);
                node.pendingWrites.addLast(ready);
                stats.writes++;
                long done = Math.max(node.lastPersistTime, now + random.nextInt(maxPersistDelay + 1));
                node.lastPersistTime = done;
                long sequence = ready.sequence();
                long incarnation = node.incarnation;
                schedule(done - now, () -> persist(node, sequence, incarnation));
            }
            // Roles first: the election of a leader is on record before its messages.
            for (Notification note : ready.notifications()) {
                if (note instanceof RoleChanged change && change.role() == Role.LEADER) {
                    observation.elected(node.id, change.term());
                    if (change.term() >= knownLeaderTerm) {
                        if (knownLeader != node.id) {
                            previousLeader = knownLeader;
                            leaderChangedAt = now;
                        }
                        knownLeaderTerm = change.term();
                        knownLeader = node.id;
                    }
                }
            }
            for (Outbound outbound : ready.messages()) {
                observation.released(node.id, outbound, node.store.hardState(), node.store.lastIndex());
                send(node, outbound);
            }
            if (ready.hasEntriesToApply()) {
                check(ready.applyTo() <= node.store.lastIndex(), "node " + node.id + " is to apply "
                        + ready.applyTo() + " but only " + node.store.lastIndex() + " entries are durable");
                for (long i = ready.applyFrom(); i <= ready.applyTo(); i++) {
                    apply(node, node.store.get(i));
                }
                serveReads(node);
            }
            for (Notification note : ready.notifications()) {
                switch (note) {
                    case ProposalAccepted accepted -> proposalAccepted(node, accepted);
                    case ProposalRejected rejected -> proposalRejected(rejected);
                    case ReadIndexReady read -> readIndexReady(node, read);
                    case ReadIndexRejected rejected -> readIndexRejected(rejected);
                    case RoleChanged _ -> {
                    }
                }
            }
        }
    }

    private static long mix(long hash, long value) {
        return (hash ^ value) * 0x100000001B3L;
    }

    /** A hash of everything in a {@code Ready} except the payload bytes of its entries. */
    private static long hashOf(Ready ready) {
        long h = mix(ready.sequence(), ready.truncateSuffixFrom());
        if (ready.hardState() != null) {
            h = mix(mix(h, ready.hardState().term()), ready.hardState().votedFor());
        }
        for (LogEntry entry : ready.entries()) {
            h = mix(mix(mix(h, entry.index()), entry.term()), entry.payload().length);
        }
        for (Outbound outbound : ready.messages()) {
            h = mix(mix(h, outbound.toNode()), outbound.message().term());
            h = switch (outbound.message()) {
                case RaftMessage.RequestVote m ->
                        mix(mix(mix(mix(h, 1), m.lastLogIndex()), m.lastLogTerm()), m.preVote() ? 1 : 0);
                case RaftMessage.VoteResponse m -> mix(mix(mix(h, 2), m.preVote() ? 1 : 0), m.granted() ? 1 : 0);
                case RaftMessage.AppendEntries m -> mix(mix(mix(mix(mix(h, 3), m.prevLogIndex()),
                        m.prevLogTerm()), m.entries().size()), m.leaderCommit());
                case RaftMessage.AppendResponse m -> mix(mix(mix(mix(mix(h, 4), m.success() ? 1 : 0),
                        m.index()), m.hintIndex()), m.hintTerm());
                case RaftMessage.Heartbeat m -> mix(mix(mix(h, 5), m.commit()), m.round());
                case RaftMessage.HeartbeatResponse m -> mix(mix(h, 6), m.round());
            };
        }
        h = mix(mix(h, ready.applyFrom()), ready.applyTo());
        for (Notification note : ready.notifications()) {
            h = switch (note) {
                case ProposalAccepted n -> mix(mix(mix(mix(h, 11), n.requestId()), n.index()), n.term());
                case ProposalRejected n ->
                        mix(mix(mix(mix(h, 12), n.requestId()), n.reason().ordinal()), n.leaderHint());
                case ReadIndexReady n -> mix(mix(mix(h, 13), n.requestId()), n.index());
                case ReadIndexRejected n ->
                        mix(mix(mix(mix(h, 14), n.requestId()), n.reason().ordinal()), n.leaderHint());
                case RoleChanged n -> mix(mix(mix(mix(h, 15), n.role().ordinal()), n.term()), n.leaderId());
            };
        }
        return h;
    }

    /** Mirrors the log changes of a {@code Ready} into the observation and checks their shape. */
    private void recordLogChange(SimNode node, Ready ready) {
        if (ready.truncateSuffixFrom() != -1) {
            check(ready.truncateSuffixFrom() >= 1, "truncation at " + ready.truncateSuffixFrom());
            stats.truncations++;
            observation.truncated(node.id, ready.truncateSuffixFrom());
        }
        for (LogEntry entry : ready.entries()) {
            List<LogEntry> log = observation.log(node.id);
            check(entry.index() == log.size() + 1,
                    "node " + node.id + ": entry " + entry + " does not follow index " + log.size());
            check(log.isEmpty() || log.getLast().term() <= entry.term(),
                    "node " + node.id + ": entry " + entry + " follows a later term");
            observation.appended(node.id, entry);
        }
    }

    private void send(SimNode from, Outbound outbound) {
        stats.messages++;
        if (random.nextInt(100) < dropPercent) {
            stats.dropped++;
            return;
        }
        transmit(from, outbound);
        if (random.nextInt(100) < duplicatePercent) {
            stats.duplicated++;
            transmit(from, outbound);
        }
    }

    private void transmit(SimNode from, Outbound outbound) {
        SimNode target = nodes.get(outbound.toNode());
        int delay = minDelay + random.nextInt(maxDelay - minDelay + 1);
        schedule(delay, () -> {
            if (blocked[from.id][target.id]) {
                stats.dropped++;
            } else {
                deliver(target, new MessageReceived(from.id, outbound.message()));
            }
        });
    }

    /** A write completes. Now and then the completion covers every write handed out so far. */
    private void persist(SimNode node, long sequence, long incarnation) {
        if (node.incarnation != incarnation || node.persistedSequence >= sequence) {
            return;
        }
        if (now < node.pausedUntil) {
            schedule(node.pausedUntil - now, () -> persist(node, sequence, incarnation));
            return;
        }
        long through = sequence;
        if (node.pendingWrites.size() > 1 && random.nextInt(3) == 0) {
            through = node.pendingWrites.peekLast().sequence();
            stats.batchedWrites++;
        }
        while (!node.pendingWrites.isEmpty() && node.pendingWrites.peekFirst().sequence() <= through) {
            node.store.store(node.pendingWrites.pollFirst());
        }
        node.persistedSequence = through;
        step(node, new Persisted(through));
    }

    // ------------------------------------------------------------------ faults

    private void faultTurn() {
        schedule(300 + random.nextInt(2200), this::faultTurn);
        if (!faults) {
            return;
        }
        List<Runnable> kinds = new ArrayList<>();
        if (options.maxPause() > 0) {
            kinds.add(this::pauseSomeNode);
        }
        if (options.crashes()) {
            kinds.add(() -> crash(someNode()));
            kinds.add(this::stallDiskAndCrash);
        }
        if (options.partitions()) {
            kinds.add(this::partition);
        }
        if (options.partitions() && options.maxPause() > 0) {
            kinds.add(this::freezeLeader);
        }
        if (!kinds.isEmpty()) {
            kinds.get(random.nextInt(kinds.size())).run();
        }
    }

    /** A random node, with a preference for the leader the clients know. */
    private SimNode someNode() {
        int target = knownLeader != RaftNode.NO_NODE && random.nextBoolean()
                ? knownLeader
                : random.nextInt(nodes.size());
        return nodes.get(target);
    }

    private void pauseSomeNode() {
        SimNode node = someNode();
        node.pausedUntil = Math.max(node.pausedUntil, now + 100 + random.nextInt(options.maxPause()));
        stats.pauses++;
    }

    /**
     * The disk of a node stops completing writes for a while, and the node crashes before
     * the disk recovers. Everything the node did in between was done on state that is lost.
     */
    private void stallDiskAndCrash() {
        SimNode node = someNode();
        if (!node.up) {
            return;
        }
        stats.diskStalls++;
        int stall = 200 + random.nextInt(600);
        node.lastPersistTime = Math.max(node.lastPersistTime, now + stall);
        long incarnation = node.incarnation;
        schedule(50 + random.nextInt(stall - 50), () -> {
            if (node.up && node.incarnation == incarnation) {
                crash(node);
            }
        });
    }

    private void crash(SimNode node) {
        if (!node.up) {
            return;
        }
        stats.crashes++;
        node.up = false;
        node.incarnation++;
        // Of the writes on their way, any prefix may have reached the disk unreported.
        int reached = random.nextInt(node.pendingWrites.size() + 1);
        for (int i = 0; i < reached; i++) {
            node.store.store(node.pendingWrites.pollFirst());
        }
        node.pendingWrites.clear();
        node.awaitingApply.clear();
        node.awaitingRead.clear();
        node.pausedUntil = 0;
        long incarnation = node.incarnation;
        schedule(50 + random.nextInt(1500), () -> {
            if (!node.up && node.incarnation == incarnation) {
                restart(node);
            }
        });
    }

    private void restart(SimNode node) {
        stats.restarts++;
        node.up = true;
        node.persistedSequence = 0;
        node.lastReadySequence = 0;
        node.lastPersistTime = now;
        node.lastSeenTerm = node.store.hardState().term();
        if (random.nextBoolean()) {
            // The state machine did not survive; it is rebuilt from the log.
            node.applied = 0;
            Arrays.fill(node.registers, 0);
        }
        check(node.applied <= node.store.lastIndex(), "node " + node.id + " had applied "
                + node.applied + " entries, but only " + node.store.lastIndex() + " are stored");
        observation.beginStep();
        observation.resetLog(node.id, node.store.entries());
        node.raft = RaftNode.create(node.config,
                new PersistedState(node.store.hardState(), SnapshotMeta.none(cluster), cluster),
                node.store, node.applied);
        checkComplete();
        if (listener != null) {
            listener.restarted(now, node.id, node.view());
        }
    }

    private void partition() {
        healPartition();
        stats.partitions++;
        int n = nodes.size();
        int kind = random.nextInt(3);
        if (kind == 0) {
            // Two groups that cannot talk to each other.
            boolean[] side = new boolean[n];
            for (int i = 0; i < n; i++) {
                side[i] = random.nextBoolean();
            }
            for (int a = 0; a < n; a++) {
                for (int b = 0; b < n; b++) {
                    blocked[a][b] = side[a] != side[b];
                }
            }
        } else {
            // One node is cut off: completely, or in one direction only.
            int lonely = someNode().id;
            boolean in = kind == 1 || random.nextBoolean();
            boolean out = kind == 1 || !in;
            for (int other = 0; other < n; other++) {
                if (other != lonely) {
                    blocked[other][lonely] = in;
                    blocked[lonely][other] = out;
                }
            }
        }
        long epoch = partitionEpoch;
        schedule(200 + random.nextInt(2000), () -> {
            if (partitionEpoch == epoch) {
                healPartition();
            }
        });
    }

    /**
     * The leader freezes, and what is sent to it in the meantime is lost instead of queued
     * (as when the buffers of a stopped process overflow). It wakes up believing that it
     * still leads, with nothing to tell it otherwise except requests of clients.
     */
    private void freezeLeader() {
        if (knownLeader == RaftNode.NO_NODE) {
            return;
        }
        healPartition();
        stats.partitions++;
        stats.pauses++;
        SimNode leader = nodes.get(knownLeader);
        for (int other = 0; other < nodes.size(); other++) {
            if (other != leader.id) {
                blocked[other][leader.id] = true;
            }
        }
        int pause = 300 + random.nextInt(500);
        leader.pausedUntil = Math.max(leader.pausedUntil, now + pause);
        long epoch = partitionEpoch;
        schedule(pause + random.nextInt(100), () -> {
            if (partitionEpoch == epoch) {
                healPartition();
            }
        });
    }

    private void healPartition() {
        partitionEpoch++;
        for (boolean[] row : blocked) {
            Arrays.fill(row, false);
        }
    }

    // ------------------------------------------------------------------ the state machine

    private static long longAt(byte[] bytes, int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= (bytes[offset + i] & 0xFFL) << (8 * i);
        }
        return value;
    }

    private static void putLong(byte[] bytes, int offset, long value) {
        for (int i = 0; i < 8; i++) {
            bytes[offset + i] = (byte) (value >>> (8 * i));
        }
    }

    private byte[] command(long commandId, byte kind, int key, long a, long b) {
        byte[] command = new byte[COMMAND_HEADER + random.nextInt(options.maxPayload() + 1)];
        putLong(command, 0, commandId);
        command[8] = kind;
        command[9] = (byte) key;
        putLong(command, 10, a);
        putLong(command, 18, b);
        return command;
    }

    private void apply(SimNode node, LogEntry entry) {
        check(entry.index() == node.applied + 1,
                "node " + node.id + " applies " + entry + " after index " + node.applied);
        boolean first = entry.index() == observation.appliedLog().size() + 1;
        observation.applied(node.id, entry);
        node.applied = entry.index();

        byte[] payload = entry.payload();
        long result = 0;
        if (payload.length >= COMMAND_HEADER) {
            int key = payload[9] & 0xFF;
            long a = longAt(payload, 10);
            if (payload[8] == COMMAND_WRITE) {
                node.registers[key] = a;
            } else if (payload[8] == COMMAND_COMPARE_AND_SET && node.registers[key] == a) {
                node.registers[key] = longAt(payload, 18);
                result = 1;
            }
            if (first) {
                commandApplied(entry, longAt(payload, 0));
            }
        }
        ClientOperation waiting = node.awaitingApply.remove(entry.index());
        if (waiting != null) {
            if (waiting.term == entry.term()) {
                respond(waiting, result);
            } else {
                waiting.discarded = true; // another entry took the place of the proposal
            }
        }
    }

    /** Called when a command is applied for the first time by any node. */
    private void commandApplied(LogEntry entry, long commandId) {
        Proposal proposal = acceptedProposals.get(commandId);
        check(proposal != null && proposal.index() == entry.index() && proposal.term() == entry.term(),
                "applied " + entry + " of command " + commandId + ", which was accepted as " + proposal);
        check(appliedCommandIds.add(commandId), "command " + commandId + " was applied twice");
        stats.appliedCommands++;
    }

    // ------------------------------------------------------------------ the load client

    private void loadTurn() {
        schedule(1 + random.nextInt(2 * options.proposalInterval()), this::loadTurn);
        if (!workload) {
            return;
        }
        propose(nextCommandId++, target(), 3);
    }

    private int target() {
        return knownLeader == RaftNode.NO_NODE || random.nextInt(5) == 0
                ? random.nextInt(nodes.size())
                : knownLeader;
    }

    private void propose(long commandId, int target, int attemptsLeft) {
        // Load goes to registers the checked client never touches.
        byte[] command = command(commandId, COMMAND_WRITE, 128 + random.nextInt(100), commandId, 0);
        stats.proposals++;
        // The request id carries the command id and the attempts left for a retry.
        deliver(nodes.get(target), new ClientPropose(commandId * 4 + attemptsLeft, command));
    }

    private void proposalAccepted(SimNode node, ProposalAccepted accepted) {
        stats.accepted++;
        boolean isChecked = accepted.requestId() >= CHECKED_BASE;
        long commandId = isChecked ? accepted.requestId() : accepted.requestId() / 4;
        Proposal earlier = acceptedProposals.put(commandId, new Proposal(accepted.index(), accepted.term()));
        check(earlier == null, "command " + commandId + " was accepted twice");
        if (isChecked) {
            ClientOperation operation = checked.get((int) (commandId - CHECKED_BASE));
            operation.index = accepted.index();
            operation.term = accepted.term();
            node.awaitingApply.put(accepted.index(), operation);
        }
    }

    private void proposalRejected(ProposalRejected rejected) {
        if (rejected.requestId() >= CHECKED_BASE) {
            checked.get((int) (rejected.requestId() - CHECKED_BASE)).discarded = true;
            return;
        }
        if (rejected.reason() == Reason.OVERLOADED) {
            stats.rejectedOverloaded++;
            return;
        }
        stats.rejectedNotLeader++;
        long commandId = rejected.requestId() / 4;
        int attemptsLeft = (int) (rejected.requestId() % 4);
        if (attemptsLeft > 0 && rejected.leaderHint() != RaftNode.NO_NODE) {
            int hint = rejected.leaderHint();
            schedule(1 + random.nextInt(maxDelay + 1), () -> propose(commandId, hint, attemptsLeft - 1));
        }
    }

    /**
     * Counts the accepted proposals whose place in the log was taken by another entry or
     * dropped. Meaningful once the cluster has converged: then every log ends where the
     * applied log ends.
     */
    public long countLostProposals() {
        long lost = 0;
        List<LogEntry> applied = observation.appliedLog();
        for (Proposal proposal : acceptedProposals.values()) {
            if (proposal.index() > applied.size()
                    || applied.get((int) proposal.index() - 1).term() != proposal.term()) {
                lost++;
            }
        }
        stats.lost = lost;
        return lost;
    }

    /** Number of accepted proposals that are neither applied nor lost. */
    public long countUndecidedProposals() {
        return stats.accepted - stats.appliedCommands - countLostProposals();
    }

    // ------------------------------------------------------------------ the checked client

    private void checkedTurn() {
        schedule(10 + random.nextInt(100), this::checkedTurn);
        if (!workload || checked.size() >= options.checkedOperations()) {
            return;
        }
        // The search of the checker grows with the number of operations that overlap, and
        // doubles with every operation that never returns. Both are kept small.
        for (Iterator<ClientOperation> iterator = outstanding.iterator(); iterator.hasNext();) {
            ClientOperation waiting = iterator.next();
            if (waiting.discarded || waiting.response != LinearizabilityChecker.NO_RESPONSE) {
                iterator.remove();
            } else if (now - waiting.invokedAt > CHECKED_PATIENCE) {
                iterator.remove();
                givenUp++;
            }
        }
        if (outstanding.size() >= CHECKED_CONCURRENCY || givenUp >= CHECKED_GIVEN_UP) {
            return;
        }
        int key = random.nextInt(CHECKED_KEYS);
        long id = CHECKED_BASE + checked.size();
        List<Long> written = valuesWritten.get(key);
        int choice = random.nextInt(10);
        ClientOperation operation;
        RaftEvent request;
        if (choice < 4) {
            issueRead(key, target());
            return;
        } else if (choice < 8) {
            operation = new ClientOperation(key, Kind.WRITE, id, 0, ++historyClock);
            request = new ClientPropose(id, command(id, COMMAND_WRITE, key, id, 0));
            written.add(id);
        } else {
            long expected = written.get(random.nextInt(written.size()));
            operation = new ClientOperation(key, Kind.COMPARE_AND_SET, expected, id, ++historyClock);
            request = new ClientPropose(id, command(id, COMMAND_COMPARE_AND_SET, key, expected, id));
            written.add(id);
        }
        operation.invokedAt = now;
        checked.add(operation);
        outstanding.add(operation);
        stats.checkedOperations++;
        deliver(nodes.get(target()), request);
    }

    private void issueRead(int key, int target) {
        long id = CHECKED_BASE + checked.size();
        ClientOperation operation = new ClientOperation(key, Kind.READ, 0, 0, ++historyClock);
        operation.invokedAt = now;
        checked.add(operation);
        outstanding.add(operation);
        stats.checkedOperations++;
        deliver(nodes.get(target), new ClientReadIndex(id));
    }

    private void respond(ClientOperation operation, long result) {
        operation.response = ++historyClock;
        operation.result = result;
        // A write that completed right after a change of leader is read back from the leader
        // of before. If that node was deposed without noticing, it must not answer from what
        // it knows: the answer would miss this write.
        if (operation.kind != Kind.READ && workload && previousLeader != RaftNode.NO_NODE
                && now - leaderChangedAt <= STALE_LEADER_WINDOW
                && checked.size() < options.checkedOperations()) {
            int key = operation.key;
            int target = previousLeader;
            stats.staleLeaderReads++;
            schedule(0, () -> issueRead(key, target)); // not inside the step of another node
        }
    }

    private void readIndexReady(SimNode node, ReadIndexReady read) {
        ClientOperation operation = checked.get((int) (read.requestId() - CHECKED_BASE));
        operation.index = read.index();
        node.awaitingRead.add(operation);
        serveReads(node);
    }

    private void readIndexRejected(ReadIndexRejected rejected) {
        stats.readsRejected++;
        checked.get((int) (rejected.requestId() - CHECKED_BASE)).discarded = true;
    }

    /** Serves the reads whose index this node has applied, from its state machine. */
    private void serveReads(SimNode node) {
        for (Iterator<ClientOperation> iterator = node.awaitingRead.iterator(); iterator.hasNext();) {
            ClientOperation operation = iterator.next();
            if (operation.index <= node.applied) {
                respond(operation, node.registers[operation.key]);
                stats.readsServed++;
                iterator.remove();
            }
        }
    }

    /**
     * The checked history. Call it once the cluster has converged: only then is it known
     * which operations without a response had no effect.
     *
     * <p>Left out are operations that were rejected, proposals that never reached a log or
     * were replaced in it, and reads that never returned. A write without a response whose
     * entry is in the final log stays in the history as an operation that may have taken
     * effect.
     */
    public List<Operation> checkedHistory() {
        List<LogEntry> applied = observation.appliedLog();
        List<Operation> history = new ArrayList<>();
        for (ClientOperation operation : checked) {
            boolean completed = operation.response != LinearizabilityChecker.NO_RESPONSE;
            if (!completed) {
                if (operation.discarded || operation.kind == Kind.READ || operation.index == -1
                        || operation.index > applied.size()
                        || applied.get((int) operation.index - 1).term() != operation.term) {
                    continue;
                }
            }
            history.add(new Operation(operation.key, operation.kind, operation.a, operation.b,
                    operation.invoke, operation.response, operation.result));
        }
        return history;
    }
}
