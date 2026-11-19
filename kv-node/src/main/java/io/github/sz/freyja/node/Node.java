package io.github.sz.freyja.node;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.List;

import io.github.sz.freyja.common.Clock;
import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.kv.Command;
import io.github.sz.freyja.common.kv.CommandCodec;
import io.github.sz.freyja.common.kv.CommandResult;
import io.github.sz.freyja.lsm.CorruptionException;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.node.raftlog.RaftLogStore;
import io.github.sz.freyja.node.sm.StateMachineStore;
import io.github.sz.freyja.node.snapshot.SnapshotTransfers;
import io.github.sz.freyja.node.store.Bootstrap;
import io.github.sz.freyja.node.store.HardStateStore;
import io.github.sz.freyja.node.store.InstallIntent;
import io.github.sz.freyja.node.store.NodeIdentity;
import io.github.sz.freyja.node.store.RepairMarker;
import io.github.sz.freyja.node.store.SnapshotMetaFile;
import io.github.sz.freyja.node.wire.AlarmCodec;
import io.github.sz.freyja.node.wire.MessageCodec;
import io.github.sz.freyja.node.wire.TransferCodec;
import io.github.sz.freyja.node.wire.TransferMessage;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.HardState;
import io.github.sz.freyja.raft.LogEntry;
import io.github.sz.freyja.raft.Notification;
import io.github.sz.freyja.raft.Outbound;
import io.github.sz.freyja.raft.PeerProgress;
import io.github.sz.freyja.raft.PersistedState;
import io.github.sz.freyja.raft.ProtocolState;
import io.github.sz.freyja.raft.RaftConfig;
import io.github.sz.freyja.raft.RaftEvent;
import io.github.sz.freyja.raft.RaftNode;
import io.github.sz.freyja.raft.Ready;
import io.github.sz.freyja.raft.Role;
import io.github.sz.freyja.raft.SnapshotDirective;
import io.github.sz.freyja.raft.SnapshotMeta;

/**
 * One node: the Raft core with its log store, its hard state, and its state machine, driven
 * by an event loop that a caller steps (docs/spec/node.md, section 1).
 *
 * <p>The node touches the outside world only through the file system and the transport it is
 * given. It has no thread and no clock of its own: whoever runs it calls {@link #tick} as time
 * passes, {@link #offer} when a message arrives, and {@link #runOnce} to process the next
 * event. Several nodes can therefore run in one deterministic simulation.
 *
 * <p>Every queue is bounded: an event that does not fit is rejected at once.
 *
 * <p>The work of a {@code Ready} is done in the order of the contract of the core: hard state,
 * truncation, entries, then one sync of the log; only then the core is told that the work is
 * durable. A failed write is fatal: the node stops and must be started again.
 *
 * <p>A node comes in two forms. The complete one owns the generations of the key-value state
 * machine and with them everything about snapshots: it shortens its log, sends snapshots to
 * nodes that are too far behind, and installs the ones it receives (section 5). The plain one
 * is given a state machine by its caller and has no snapshots; its log only grows.
 *
 * <p>An instance is confined to one thread.
 */
public final class Node implements Closeable {

    /** Subdirectory of the data directory that holds the Raft log. */
    public static final String RAFT_DIRECTORY = "raft";

    /**
     * The settings of a node.
     *
     * @param raft                  the settings of the Raft core; its node id is replaced by
     *                              the one of the identity file
     * @param segmentSize           target size of a segment of the Raft log
     * @param queueCapacity         how many events may wait for the event loop
     * @param snapshotLogBytes      when the log takes more than this on disk, the state
     *                              machine is made durable and the log is shortened
     * @param snapshotRetainEntries how many applied entries the log keeps then, for followers
     *                              that are a little behind
     * @param transfers             the settings of snapshot transfers
     * @param limits                the limits that protect the node and the cluster
     * @param clock                 the time source for what the node measures
     */
    public record Options(RaftConfig raft, long segmentSize, int queueCapacity, long snapshotLogBytes,
                          long snapshotRetainEntries, SnapshotTransfers.Options transfers, Limits limits,
                          Clock clock) {
        public Options {
            if (segmentSize < 1 || queueCapacity < 1 || snapshotLogBytes < 1 || snapshotRetainEntries < 0) {
                throw new IllegalArgumentException("segment size " + segmentSize + ", queue capacity "
                        + queueCapacity + ", snapshot at " + snapshotLogBytes + " bytes, retaining "
                        + snapshotRetainEntries);
            }
        }

        /** Snapshots and limits as by default: at 1 GiB of log, keeping 10,000 entries. */
        public Options(RaftConfig raft, long segmentSize, int queueCapacity) {
            this(raft, segmentSize, queueCapacity, 1L << 30, 10_000, SnapshotTransfers.Options.defaults(),
                    Limits.defaults(), Clock.SYSTEM);
        }

        /** The defaults: 64 MiB segments, 4096 waiting events. */
        public static Options defaults() {
            return new Options(RaftConfig.defaults(0, 0), 64L << 20, 4096);
        }
    }

    /**
     * The limits that protect the node and the cluster (sections 4.4 and 7).
     *
     * @param maxApplyLag     proposals are rejected while the state machine is more than this
     *                        many entries behind the commit index
     * @param quotaBytes      when the state machine holds more than this, the node raises the
     *                        alarm for lack of space
     * @param minFreeBytes    when the file system of the data directory has less than this
     *                        free, the node raises the same alarm
     * @param alarmCheckTicks how often the node looks at the two conditions, and how long it
     *                        waits before it raises the alarm again
     * @param slowSyncNanos   a leader that needs longer than this to make the work of one
     *                        {@code Ready} durable gives its leadership up
     */
    public record Limits(long maxApplyLag, long quotaBytes, long minFreeBytes, int alarmCheckTicks,
                         long slowSyncNanos) {
        public Limits {
            if (maxApplyLag < 1 || quotaBytes < 1 || minFreeBytes < 0 || alarmCheckTicks < 1
                    || slowSyncNanos < 1) {
                throw new IllegalArgumentException("apply lag " + maxApplyLag + ", quota " + quotaBytes
                        + ", free " + minFreeBytes + ", check every " + alarmCheckTicks + " ticks, slow sync "
                        + slowSyncNanos);
            }
        }

        /** The defaults: 10,000 entries, 16 GiB, 1 GiB free, a look every 10 ticks, 1 second. */
        public static Limits defaults() {
            return new Limits(10_000, 16L << 30, 1L << 30, 10, 1_000_000_000L);
        }
    }

    /** What became of a proposal when it was handed to the node. */
    public enum Admission {
        /** It waits for the event loop. Its answer comes through the listener. */
        ACCEPTED,
        /** The queue is full, the node has stopped, or the state machine is too far behind. */
        OVERLOADED,
        /** Its deadline has passed. */
        EXPIRED
    }

    /** A proposal that waits for the event loop. */
    private record Proposal(long requestId, byte[] command, long deadline) {
    }

    /** A received snapshot that waits for the verdict of the core. */
    private record Staged(TransferMessage.Offer offer, long generation) {
    }

    /** The queue item that asks for a snapshot now. */
    private static final Object SNAPSHOT_REQUEST = new Object();

    private final StorageFileSystem fs;
    private final Path dataDirectory;
    private final Options options;
    private final NodeIdentity identity;
    private final HardStateStore hardState;
    private final RaftLogStore log;
    private final RaftNode raft;
    private final StateMachine stateMachine;
    /** The generations of the state machine; {@code null} for a plain node. */
    private final StateMachineStore store;
    private final Transport transport;
    private final NodeListener listener;
    private final boolean observed;
    private final ArrayDeque<Object> queue = new ArrayDeque<>();
    private SnapshotTransfers transfers;
    /** What the stored log starts after. */
    private SnapshotMeta snapshot;
    private Staged staged;
    private long snapshotsCreated;
    private long snapshotsInstalled;
    private boolean repairedAtStart;
    private boolean lostTailWitnessed;
    /** The time of the node: the number of ticks it has processed. */
    private long now;
    private long expiredProposals;
    private long slowSyncStepDowns;
    private long alarmsRaised;
    /** Request ids of the proposals the node makes itself; clients use ids from 0 on. */
    private long ownRequestId = -1;
    private long rejectedEvents;
    private long invalidMessages;
    private boolean stopped;

    private Node(StorageFileSystem fs, Path dataDirectory, Options options, NodeIdentity identity,
                 HardStateStore hardState, RaftLogStore log, RaftNode raft, StateMachine stateMachine,
                 StateMachineStore store, Transport transport, NodeListener listener, SnapshotMeta snapshot) {
        this.fs = fs;
        this.dataDirectory = dataDirectory;
        this.options = options;
        this.identity = identity;
        this.hardState = hardState;
        this.log = log;
        this.raft = raft;
        this.stateMachine = stateMachine;
        this.store = store;
        this.transport = transport;
        this.listener = listener;
        this.snapshot = snapshot;
        this.observed = listener.observesCore();
    }

    // ------------------------------------------------------------------ start

    /**
     * Prepares an empty data directory for a plain node that has never run: writes its
     * identity and its first hard state.
     */
    public static void initialize(StorageFileSystem fs, Path dataDirectory, NodeIdentity identity)
            throws IOException {
        fs.createDirectories(dataDirectory);
        if (NodeIdentity.exists(fs, dataDirectory)) {
            throw new IOException(dataDirectory + " holds a node already");
        }
        HardStateStore.initialize(fs, dataDirectory);
        // The identity comes last: a directory without it was never completely prepared.
        identity.write(fs, dataDirectory);
    }

    /**
     * Prepares an empty data directory for a complete node that has never run: its first hard
     * state, an empty state machine, and its identity.
     *
     * @param config         the configuration the cluster starts with, the same on every node
     *                       of a new cluster; empty for a node that is to join a cluster
     * @param clusterVersion the cluster version the cluster starts with
     */
    public static void initialize(StorageFileSystem fs, Path dataDirectory, NodeIdentity identity,
                                  ClusterConfig config, int clusterVersion) throws IOException {
        fs.createDirectories(dataDirectory);
        if (NodeIdentity.exists(fs, dataDirectory)) {
            throw new IOException(dataDirectory + " holds a node already");
        }
        HardStateStore.initialize(fs, dataDirectory);
        new Bootstrap(config, clusterVersion).write(fs, dataDirectory);
        SnapshotMetaFile.write(fs, dataDirectory, SnapshotMeta.none(config));
        StateMachineStore.initialize(fs, dataDirectory, config, clusterVersion);
        // The identity comes last: a directory without it was never completely prepared.
        identity.write(fs, dataDirectory);
    }

    /**
     * Starts a plain node from its data directory, with a state machine of the caller.
     *
     * @param initialConfig the configuration the cluster started with
     * @param listener      is told what the core reports: answers to proposals, changes of role
     * @throws io.github.sz.freyja.node.store.LostIdentityException if the node cannot
     *         know who it is or what it has promised (section 3.4)
     */
    public static Node open(StorageFileSystem fs, Path dataDirectory, Options options,
                            ClusterConfig initialConfig, StateMachine stateMachine, Transport transport,
                            NodeListener listener) throws IOException {
        NodeIdentity identity = NodeIdentity.read(fs, dataDirectory);
        HardStateStore hardState = HardStateStore.open(fs, dataDirectory);
        return start(fs, dataDirectory, options, identity, hardState, SnapshotMeta.none(initialConfig),
                stateMachine, null, transport, listener);
    }

    /**
     * Starts a complete node from its data directory.
     *
     * <p>An installation of a snapshot that a crash interrupted is completed first. A state
     * machine that was found damaged, earlier or now, is given up and rebuilt (section 6). If
     * the log is damaged, the node makes its hard state say that it is in log-recovering mode
     * and only then sets the damaged segments aside (section 3.3).
     *
     * @throws io.github.sz.freyja.node.store.LostIdentityException if the node cannot
     *         know who it is or what it has promised (section 3.4)
     */
    public static Node open(StorageFileSystem fs, Path dataDirectory, Options options,
                            StateMachineStore.Options machine, Transport transport, NodeListener listener)
            throws IOException {
        NodeIdentity identity = NodeIdentity.read(fs, dataDirectory);
        HardStateStore hardState = HardStateStore.open(fs, dataDirectory);
        InstallIntent intent = InstallIntent.read(fs, dataDirectory);
        if (intent != null) {
            // Every step has no effect if it was done before the crash.
            if (intent.replaceStateMachine()) {
                StateMachineStore.adopt(fs, dataDirectory, intent.generation(), intent.meta(),
                        intent.clusterVersion());
            }
            SnapshotMetaFile.write(fs, dataDirectory, intent.meta());
            if (!intent.keepSuffix()) {
                RaftLogStore.deleteSegments(fs, dataDirectory.resolve(RAFT_DIRECTORY));
            }
            InstallIntent.clear(fs, dataDirectory);
        }
        boolean repaired = RepairMarker.exists(fs, dataDirectory);
        StateMachineStore store = null;
        if (!repaired) {
            try {
                store = StateMachineStore.open(fs, dataDirectory, machine);
            } catch (CorruptionException e) {
                // The state machine does not open. From here on it is not looked at again.
                RepairMarker.write(fs, dataDirectory);
                repaired = true;
            }
        }
        if (repaired) {
            giveUpStateMachine(fs, dataDirectory, options, identity, hardState);
            store = StateMachineStore.open(fs, dataDirectory, machine);
        }
        boolean opened = false;
        try {
            SnapshotMeta snapshot = SnapshotMetaFile.read(fs, dataDirectory);
            if (snapshot == null) {
                // The file is lost. The state machine itself is a snapshot the log leads up
                // to: the log gave up only entries that the state machine held durably.
                snapshot = store.meta();
                SnapshotMetaFile.write(fs, dataDirectory, snapshot);
            }
            Node node = start(fs, dataDirectory, options, identity, hardState, snapshot, store, store,
                    transport, listener);
            node.repairedAtStart = repaired;
            opened = true;
            return node;
        } finally {
            if (!opened) {
                store.close();
            }
        }
    }

    /**
     * Replaces a damaged state machine by an empty one (section 6). The marker is there when
     * this starts and is removed when it is done, so a node that crashes in between does all
     * of it again; every step can be repeated.
     *
     * <p>If the log reaches back to its first entry, the node applies it again from there.
     * Otherwise the log is of no use without the state it continues, and it goes too: the
     * node enters log-recovering mode, durably and before the log is touched, because it has
     * acknowledged entries that it will no longer hold. The leader then sends its state
     * machine and the entries after it.
     */
    private static void giveUpStateMachine(StorageFileSystem fs, Path dataDirectory, Options options,
                                           NodeIdentity identity, HardStateStore hardState)
            throws IOException {
        Bootstrap bootstrap = Bootstrap.read(fs, dataDirectory);
        if (bootstrap == null) {
            throw new IOException(dataDirectory.resolve(Bootstrap.FILE_NAME) + " is missing or damaged; "
                    + "the state machine cannot be rebuilt. Replace the member.");
        }
        SnapshotMeta snapshot = SnapshotMetaFile.read(fs, dataDirectory);
        if (snapshot == null || snapshot.lastIncludedIndex() > 0) {
            if (snapshot != null) {
                // Through the core, which notes in the hard state what the lost log held: with
                // that the node can still vote as it would have voted with its log.
                long index = snapshot.lastIncludedIndex();
                StateMachine none = new StateMachine() {
                    @Override
                    public long appliedIndex() {
                        return index;
                    }

                    @Override
                    public List<CommandResult> apply(List<LogEntry> entries) {
                        throw new IllegalStateException("nothing is applied while the state machine is given up");
                    }
                };
                Node node = start(fs, dataDirectory, options, identity, hardState, snapshot, none, null,
                        (to, message) -> {
                        }, notification -> {
                        });
                try {
                    node.feed(new RaftEvent.LogDamaged(index + 1));
                    node.drain();
                } finally {
                    node.log.close();
                }
            } else {
                // Where the log starts is not known either. The node says that it lost a log
                // it knows nothing about, and gives no vote until it is refilled.
                HardState stored = hardState.get();
                hardState.put(new HardState(stored.term(), stored.votedFor(), true, stored.logEpoch() + 1, 0,
                        HardState.UNKNOWN_TERM));
            }
            SnapshotMetaFile.write(fs, dataDirectory, SnapshotMeta.none(bootstrap.config()));
            RaftLogStore.deleteSegments(fs, dataDirectory.resolve(RAFT_DIRECTORY));
        }
        StateMachineStore.replaceByEmpty(fs, dataDirectory, bootstrap);
        RepairMarker.clear(fs, dataDirectory);
    }

    private static Node start(StorageFileSystem fs, Path dataDirectory, Options options, NodeIdentity identity,
                              HardStateStore hardState, SnapshotMeta snapshot, StateMachine stateMachine,
                              StateMachineStore store, Transport transport, NodeListener listener)
            throws IOException {
        long snapshotIndex = snapshot.lastIncludedIndex();
        RaftLogStore log = RaftLogStore.open(fs, dataDirectory.resolve(RAFT_DIRECTORY), options.segmentSize(),
                snapshotIndex, snapshot.config());
        boolean opened = false;
        try {
            // Every node gets its own jitter: with one seed for all, the election timers of
            // all nodes would fire together, election after election.
            RaftConfig config = options.raft().withNode(identity.nodeId(),
                    options.raft().seed() ^ (identity.nodeId() + 1) * 0x9E3779B97F4A7C15L);
            long applied = stateMachine.appliedIndex();
            long damagedFrom = log.firstDamagedIndex();
            HardState stored = hardState.get();
            // The state machine is a witness: an entry is applied only after it is durable in
            // the log. If the state machine holds more than the log, the log has lost entries
            // that were synced, though nothing in the log shows it (damage after the last
            // sync point looks like a torn tail). The node treats that as damage.
            boolean witnessed = damagedFrom == -1 && !stored.logRecovering()
                    && applied > Math.max(log.lastIndex(), snapshotIndex);
            if (witnessed) {
                damagedFrom = log.lastIndex() + 1;
            }
            // With a damaged log the state machine may be ahead of what the log still holds.
            // The core accepts that only in log-recovering mode, which the report of the
            // damage below is about to start; the flag is durable before anything else happens.
            HardState start = damagedFrom == -1 ? stored : new HardState(stored.term(), stored.votedFor(), true,
                    stored.logEpoch(), stored.lostLogIndex(), stored.lostLogTerm());
            RaftNode raft = RaftNode.create(config, new PersistedState(start, snapshot), log, applied);
            Node node = new Node(fs, dataDirectory, options, identity, hardState, log, raft, stateMachine, store,
                    transport, listener, snapshot);
            if (damagedFrom != -1) {
                // Entries that the snapshot covers are not missed, wherever the damage begins.
                node.feed(new RaftEvent.LogDamaged(Math.max(damagedFrom, snapshotIndex + 1)));
                node.drain();
                if (!witnessed) {
                    log.quarantineDamaged();
                }
                node.lostTailWitnessed = witnessed;
                if (log.lastIndex() < snapshotIndex) {
                    // What is left ends before the snapshot: nothing of it is needed, and the
                    // next entry to store is the one after the snapshot.
                    log.reset(snapshotIndex, snapshot.config());
                }
            }
            if (store != null) {
                node.transfers = SnapshotTransfers.open(fs, dataDirectory, store, options.transfers(),
                        node.new TransferHost());
            }
            opened = true;
            return node;
        } finally {
            if (!opened) {
                log.close();
            }
        }
    }

    /** Hands an event to the core, and shows the step to a listener that wants to see it. */
    private void feed(RaftEvent event) {
        raft.step(event);
        if (observed) {
            listener.stepped(event, raft.view());
        }
    }

    // ------------------------------------------------------------------ input

    /** One unit of time has passed. */
    public boolean tick() {
        return enqueue(new RaftEvent.Tick());
    }

    /**
     * A message from another node has arrived.
     *
     * @return whether the message was accepted; a message that is damaged, not meant for this
     *         node, or does not fit into the queue is dropped
     */
    public boolean offer(byte[] message) {
        try {
            if (AlarmCodec.isAlarm(message)) {
                AlarmCodec.Request request = AlarmCodec.decode(message);
                if (request.to() != identity.nodeId() || store == null) {
                    invalidMessages++;
                    return false;
                }
                return enqueue(request);
            }
            if (TransferCodec.isTransfer(message)) {
                TransferCodec.Envelope envelope = TransferCodec.decode(message);
                if (envelope.to() != identity.nodeId() || transfers == null) {
                    invalidMessages++;
                    return false;
                }
                return enqueue(envelope);
            }
            MessageCodec.Envelope envelope = MessageCodec.decode(message);
            if (envelope.to() != identity.nodeId()) {
                invalidMessages++;
                return false;
            }
            return enqueue(new RaftEvent.MessageReceived(envelope.from(), envelope.message()));
        } catch (DecodeException e) {
            invalidMessages++;
            return false;
        }
    }

    /** Hands an event of a client or an administrator to the core. */
    public boolean submit(RaftEvent event) {
        return enqueue(event);
    }

    /**
     * Hands a command of a client to the node (section 7). A command that cannot be taken is
     * rejected at once; one that waits beyond its deadline is dropped before it is proposed.
     *
     * @param requestId identifies the proposal in the answers of the core; not negative
     * @param deadline  the time of the node ({@link #now}) after which nobody waits for the
     *                  answer
     */
    public Admission propose(long requestId, byte[] command, long deadline) {
        if (requestId < 0) {
            throw new IllegalArgumentException("request id " + requestId);
        }
        if (deadline < now) {
            expiredProposals++;
            return Admission.EXPIRED;
        }
        if (!stopped && raft.view().commitIndex() - stateMachine.appliedIndex() > options.limits().maxApplyLag()) {
            rejectedEvents++;
            return Admission.OVERLOADED;
        }
        return enqueue(new Proposal(requestId, command, deadline)) ? Admission.ACCEPTED : Admission.OVERLOADED;
    }

    /**
     * Asks the node to make its state machine durable and to shorten its log now, without
     * keeping entries for followers.
     *
     * @return whether the request was accepted; a plain node has no snapshots
     */
    public boolean requestSnapshot() {
        return store != null && enqueue(SNAPSHOT_REQUEST);
    }

    private boolean enqueue(Object item) {
        if (stopped || queue.size() >= options.queueCapacity()) {
            rejectedEvents++;
            return false;
        }
        queue.addLast(item);
        return true;
    }

    // ------------------------------------------------------------------ the event loop

    /**
     * Processes the next waiting event and everything that follows from it.
     *
     * @return whether an event was processed
     * @throws IOException if storage failed; the node has stopped then
     */
    public boolean runOnce() throws IOException {
        if (stopped) {
            throw new IOException("the node has stopped after a storage failure");
        }
        Object item = queue.pollFirst();
        if (item == null) {
            return false;
        }
        try {
            if (item instanceof RaftEvent event) {
                if (event instanceof RaftEvent.Tick) {
                    now++;
                    if (transfers != null) {
                        transfers.tick();
                    }
                    if (store != null && now % options.limits().alarmCheckTicks() == 0) {
                        checkSpace();
                    }
                }
                feed(event);
            } else if (item instanceof TransferCodec.Envelope envelope) {
                transfers.handle(envelope.from(), envelope.message());
            } else if (item instanceof Proposal proposal) {
                if (proposal.deadline() < now) {
                    expiredProposals++;
                    listener.proposalExpired(proposal.requestId());
                } else {
                    feed(new RaftEvent.ClientPropose(proposal.requestId(), proposal.command()));
                }
            } else if (item instanceof AlarmCodec.Request request) {
                raiseAlarm(request.alarm());
            }
            work();
            if (item == SNAPSHOT_REQUEST) {
                createSnapshot(0);
            } else if (store != null && log.segmentCount() > 1
                    && log.storedBytes() > options.snapshotLogBytes()
                    && store.appliedIndex() - options.snapshotRetainEntries()
                    >= snapshot.lastIncludedIndex() + Math.max(1, options.snapshotRetainEntries())) {
                createSnapshot(options.snapshotRetainEntries());
            }
            if (staged != null) {
                // The core gave no verdict on a received snapshot; it has no use for it.
                store.discard(staged.generation());
                staged = null;
            }
            if (store != null && store.damage() != null) {
                // A scrub or a read found damage. Nothing more is applied to this state machine.
                stateMachineDamaged();
                throw store.damage();
            }
        } catch (IOException | RuntimeException e) {
            // Whatever failed, the state in memory may be ahead of the state on disk.
            stopped = true;
            queue.clear();
            throw e;
        }
        return true;
    }

    /** Does the work the core has ready, until it has none; a failure stops the node. */
    private void drain() throws IOException {
        try {
            work();
        } catch (IOException | RuntimeException e) {
            stopped = true;
            queue.clear();
            throw e;
        }
    }

    private void work() throws IOException {
        while (raft.hasReady()) {
            Ready ready = raft.takeReady();
            if (ready.hasPersistentWork()) {
                long started = options.clock().nanoTime();
                if (ready.hardState() != null) {
                    hardState.put(ready.hardState());
                }
                boolean logChanged = false;
                if (ready.truncateSuffixFrom() != -1) {
                    log.truncateSuffix(ready.truncateSuffixFrom());
                    logChanged = true;
                }
                if (!ready.entries().isEmpty()) {
                    log.append(ready.entries());
                    logChanged = true;
                }
                if (logChanged) {
                    log.sync();
                }
                long took = options.clock().nanoTime() - started;
                feed(new RaftEvent.Persisted(ready.sequence()));
                if (took > options.limits().slowSyncNanos() && raft.view().role() == Role.LEADER) {
                    // A node with a healthy disk should lead (section 7).
                    slowSyncStepDowns++;
                    feed(new RaftEvent.StepDown());
                }
            }
            if (observed) {
                listener.ready(ready, hardState.get(), log.lastIndex());
            }
            for (Outbound outbound : ready.messages()) {
                transport.send(outbound.toNode(),
                        MessageCodec.encode(identity.nodeId(), outbound.toNode(), outbound.message()));
            }
            if (ready.hasEntriesToApply()) {
                List<LogEntry> entries = log.entries(ready.applyFrom(), ready.applyTo() + 1, Integer.MAX_VALUE);
                List<CommandResult> results;
                try {
                    results = stateMachine.apply(entries);
                } catch (CorruptionException e) {
                    stateMachineDamaged();
                    throw e;
                }
                for (int i = 0; i < entries.size(); i++) {
                    listener.applied(entries.get(i), results.get(i));
                }
            }
            for (Notification note : ready.notifications()) {
                listener.notification(note);
            }
            if (ready.truncatePrefixThrough() != -1) {
                // The file SNAPSHOT names this index or a later one already.
                log.truncatePrefix(ready.truncatePrefixThrough());
            }
            for (SnapshotDirective directive : ready.snapshots()) {
                switch (directive) {
                    case SnapshotDirective.Send send -> {
                        if (transfers == null) {
                            feed(new RaftEvent.SnapshotSendFinished(send.toNode(), false));
                        } else {
                            transfers.send(send.toNode(), send.term());
                        }
                    }
                    case SnapshotDirective.Install install -> install(install);
                    case SnapshotDirective.Reject reject -> {
                        if (staged != null && staged.offer().meta().equals(reject.meta())) {
                            store.discard(staged.generation());
                            staged = null;
                        }
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------ alarms

    /** Raises the alarm for lack of space if the state machine or the disk is too full. */
    private void checkSpace() throws IOException {
        if (store.machine().alarms().contains(Command.Alarm.NOSPACE)) {
            return;
        }
        if (store.machine().engine().storedBytes() > options.limits().quotaBytes()
                || fs.freeSpace(dataDirectory) < options.limits().minFreeBytes()) {
            raiseAlarm(Command.Alarm.NOSPACE);
        }
    }

    /**
     * Gets an alarm into the replicated state: the leader proposes it, every other node asks
     * the leader to. Nothing is remembered: while the condition lasts, the node asks again.
     */
    private void raiseAlarm(Command.Alarm alarm) {
        if (store == null || store.machine().alarms().contains(alarm)) {
            return;
        }
        ProtocolState state = raft.view();
        if (state.role() == Role.LEADER) {
            alarmsRaised++;
            // Time stamp 0: the command of a node does not move the time of the state machine.
            feed(new RaftEvent.ClientPropose(ownRequestId--,
                    CommandCodec.encode(0, new Command.SetAlarm(alarm))));
        } else if (state.leaderId() >= 0 && state.leaderId() != identity.nodeId()) {
            transport.send(state.leaderId(), AlarmCodec.encode(identity.nodeId(), state.leaderId(), alarm));
        }
    }

    /**
     * Records that the state machine is damaged and stops the node (section 6). The node
     * rebuilds the state machine at its next start. For whoever reads the state machine and
     * finds damage there; the event loop calls it for damage that it meets itself.
     */
    public void stateMachineDamaged() throws IOException {
        if (store != null) {
            RepairMarker.write(fs, dataDirectory);
        }
        stopped = true;
        queue.clear();
    }

    // ------------------------------------------------------------------ snapshots

    /**
     * Makes the state machine durable and lets the log start after an applied entry.
     *
     * @param retain how many applied entries the log keeps
     */
    private void createSnapshot(long retain) throws IOException {
        // A node whose log was damaged can have applied more than its log holds.
        long index = Math.min(store.appliedIndex(), log.lastIndex()) - retain;
        if (index <= snapshot.lastIncludedIndex()) {
            return;
        }
        // The log gives up only what the state machine holds durably: first the state
        // machine, then the file that says where the log starts, then the log.
        store.flush();
        SnapshotMeta meta = raft.snapshotMeta(index);
        SnapshotMetaFile.write(fs, dataDirectory, meta);
        snapshot = meta;
        snapshotsCreated++;
        feed(new RaftEvent.SnapshotCreated(meta));
        work();
    }

    /**
     * Installs the received snapshot the core has decided for (section 5). The intent is
     * durable before the first step and removed after the last, so a node that crashes in
     * between completes the installation when it starts.
     */
    private void install(SnapshotDirective.Install install) throws IOException {
        Staged received = staged;
        SnapshotMeta meta = install.meta();
        if (received == null || !received.offer().meta().equals(meta)) {
            throw new IllegalStateException("no received snapshot for " + meta);
        }
        staged = null;
        int clusterVersion = received.offer().clusterVersion();
        new InstallIntent(meta, received.generation(), clusterVersion, install.keepSuffix(),
                install.replaceStateMachine()).write(fs, dataDirectory);
        if (install.replaceStateMachine()) {
            store.install(received.generation(), meta, clusterVersion);
        } else {
            store.discard(received.generation());
        }
        SnapshotMetaFile.write(fs, dataDirectory, meta);
        if (install.keepSuffix()) {
            log.truncatePrefix(meta.lastIncludedIndex());
        } else {
            log.reset(meta.lastIncludedIndex(), meta.config());
        }
        InstallIntent.clear(fs, dataDirectory);
        snapshot = meta;
        snapshotsInstalled++;
        feed(new RaftEvent.SnapshotInstalled(meta));
        if (install.replaceStateMachine()) {
            listener.snapshotInstalled(meta);
        }
    }

    /** What the snapshot transfers need from the node. */
    private final class TransferHost implements SnapshotTransfers.Host {

        @Override
        public void send(int toNode, TransferMessage message) {
            transport.send(toNode, TransferCodec.encode(identity.nodeId(), toNode, message));
        }

        @Override
        public boolean wanted(int fromNode, TransferMessage.Offer offer) {
            ProtocolState state = raft.view();
            return staged == null && offer.leaderTerm() >= state.term()
                    && offer.meta().lastIncludedIndex() > state.commitIndex();
        }

        @Override
        public void received(int fromNode, TransferMessage.Offer offer, long generation) {
            staged = new Staged(offer, generation);
            feed(new RaftEvent.SnapshotOffered(fromNode, offer.leaderTerm(), offer.meta()));
        }

        @Override
        public void sendFinished(int toNode, boolean success) {
            feed(new RaftEvent.SnapshotSendFinished(toNode, success));
        }
    }

    // ------------------------------------------------------------------ status

    public NodeIdentity identity() {
        return identity;
    }

    /** The protocol state of the Raft core. */
    public ProtocolState view() {
        return raft.view();
    }

    /** What this node, as leader, knows of the other members; empty if it is not the leader. */
    public List<PeerProgress> peers() {
        return raft.peers();
    }

    /** The generations of the state machine; {@code null} for a plain node. */
    public StateMachineStore store() {
        return store;
    }

    /** What the stored log starts after. */
    public SnapshotMeta snapshot() {
        return snapshot;
    }

    /** The snapshot transfers; {@code null} for a plain node. */
    public SnapshotTransfers transfers() {
        return transfers;
    }

    /** How often the node has shortened its log after making the state machine durable. */
    public long snapshotsCreated() {
        return snapshotsCreated;
    }

    /** How often the node has installed a snapshot of another node. */
    public long snapshotsInstalled() {
        return snapshotsInstalled;
    }

    /** The time of the node: the number of ticks it has processed. Deadlines are given in it. */
    public long now() {
        return now;
    }

    /** Proposals dropped or refused because their deadline had passed. */
    public long expiredProposals() {
        return expiredProposals;
    }

    /** How often this node has given its leadership up because its disk was slow. */
    public long slowSyncStepDowns() {
        return slowSyncStepDowns;
    }

    /** How often this node, as leader, has proposed an alarm. */
    public long alarmsRaised() {
        return alarmsRaised;
    }

    /**
     * Whether this start found the state machine ahead of an undamaged log and therefore
     * entered log-recovering mode: the log has lost entries that were synced.
     */
    public boolean lostTailWitnessed() {
        return lostTailWitnessed;
    }

    /** Whether this start replaced a damaged state machine by an empty one. */
    public boolean repairedAtStart() {
        return repairedAtStart;
    }

    /**
     * The stored entries from {@code fromIndex} on, at most {@code maxBytes} of them. For
     * simulations and tools; the node itself reads its log through the core.
     */
    public List<LogEntry> storedEntries(long fromIndex, int maxBytes) {
        return log.entries(Math.max(fromIndex, log.firstIndex()), log.lastIndex() + 1, maxBytes);
    }

    /** The first index the log store holds. */
    public long firstStoredIndex() {
        return log.firstIndex();
    }

    /** The number of segment files of the log. */
    public int logSegmentCount() {
        return log.segmentCount();
    }

    /** The bytes the segments of the log take on disk. */
    public long logStoredBytes() {
        return log.storedBytes();
    }

    /** The number of events that wait for the event loop. */
    public int waitingEvents() {
        return queue.size();
    }

    /** Events rejected because the queue was full or the node had stopped. */
    public long rejectedEvents() {
        return rejectedEvents;
    }

    /** Messages dropped because they were damaged or meant for another node. */
    public long invalidMessages() {
        return invalidMessages;
    }

    /** Whether the node has stopped after a storage failure. */
    public boolean stopped() {
        return stopped;
    }

    /** Closes the log, the transfers, and the state machine if the node owns it. */
    @Override
    public void close() throws IOException {
        stopped = true;
        queue.clear();
        IOException first = null;
        try {
            if (transfers != null) {
                transfers.close();
            }
        } catch (IOException e) {
            first = e;
        }
        try {
            log.close();
        } catch (IOException e) {
            first = first == null ? e : first;
        }
        try {
            if (store != null) {
                store.close();
            }
        } catch (IOException e) {
            first = first == null ? e : first;
        }
        if (first != null) {
            throw first;
        }
    }
}
