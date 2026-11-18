package io.github.stefan_zobel.freyja.node;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.List;

import io.github.stefan_zobel.freyja.common.DecodeException;
import io.github.stefan_zobel.freyja.common.kv.CommandResult;
import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;
import io.github.stefan_zobel.freyja.node.raftlog.RaftLogStore;
import io.github.stefan_zobel.freyja.node.store.HardStateStore;
import io.github.stefan_zobel.freyja.node.store.NodeIdentity;
import io.github.stefan_zobel.freyja.node.wire.MessageCodec;
import io.github.stefan_zobel.freyja.raft.ClusterConfig;
import io.github.stefan_zobel.freyja.raft.HardState;
import io.github.stefan_zobel.freyja.raft.LogEntry;
import io.github.stefan_zobel.freyja.raft.Notification;
import io.github.stefan_zobel.freyja.raft.Outbound;
import io.github.stefan_zobel.freyja.raft.PersistedState;
import io.github.stefan_zobel.freyja.raft.ProtocolState;
import io.github.stefan_zobel.freyja.raft.RaftConfig;
import io.github.stefan_zobel.freyja.raft.RaftEvent;
import io.github.stefan_zobel.freyja.raft.RaftNode;
import io.github.stefan_zobel.freyja.raft.Ready;
import io.github.stefan_zobel.freyja.raft.SnapshotMeta;

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
 * <p>An instance is confined to one thread.
 */
public final class Node implements Closeable {

    /** Subdirectory of the data directory that holds the Raft log. */
    public static final String RAFT_DIRECTORY = "raft";

    /**
     * The settings of a node.
     *
     * @param raft          the settings of the Raft core; its node id is replaced by the one of
     *                      the identity file
     * @param segmentSize   target size of a segment of the Raft log
     * @param queueCapacity how many events may wait for the event loop
     */
    public record Options(RaftConfig raft, long segmentSize, int queueCapacity) {
        public Options {
            if (segmentSize < 1 || queueCapacity < 1) {
                throw new IllegalArgumentException("segment size " + segmentSize + ", queue capacity "
                        + queueCapacity);
            }
        }

        /** The defaults: 64 MiB segments, 4096 waiting events. */
        public static Options defaults() {
            return new Options(RaftConfig.defaults(0, 0), 64L << 20, 4096);
        }
    }

    private final NodeIdentity identity;
    private final HardStateStore hardState;
    private final RaftLogStore log;
    private final RaftNode raft;
    private final StateMachine stateMachine;
    private final Transport transport;
    private final NodeListener listener;
    private final ArrayDeque<RaftEvent> queue = new ArrayDeque<>();
    private final int queueCapacity;
    private long rejectedEvents;
    private long invalidMessages;
    private boolean stopped;

    private Node(NodeIdentity identity, HardStateStore hardState, RaftLogStore log, RaftNode raft,
                 StateMachine stateMachine, Transport transport, NodeListener listener,
                 int queueCapacity) {
        this.identity = identity;
        this.hardState = hardState;
        this.log = log;
        this.raft = raft;
        this.stateMachine = stateMachine;
        this.transport = transport;
        this.listener = listener;
        this.queueCapacity = queueCapacity;
    }

    /**
     * Prepares an empty data directory for a node that has never run: writes its identity and
     * its first hard state.
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
     * Starts the node from its data directory.
     *
     * <p>If the log is damaged, the node first makes its hard state say that it is in
     * log-recovering mode and only then sets the damaged segments aside (section 3.3).
     *
     * @param initialConfig the configuration the cluster started with; an empty one for a
     *                      node that is to join. (The state machine will hold the
     *                      configuration of its snapshot once it stores one.)
     * @param listener      is told what the core reports: answers to proposals, changes of role
     * @throws io.github.stefan_zobel.freyja.node.store.LostIdentityException if the node cannot
     *         know who it is or what it has promised (section 3.4)
     */
    public static Node open(StorageFileSystem fs, Path dataDirectory, Options options,
                            ClusterConfig initialConfig, StateMachine stateMachine, Transport transport,
                            NodeListener listener) throws IOException {
        NodeIdentity identity = NodeIdentity.read(fs, dataDirectory);
        HardStateStore hardState = HardStateStore.open(fs, dataDirectory);
        RaftLogStore log = RaftLogStore.open(fs, dataDirectory.resolve(RAFT_DIRECTORY), options.segmentSize(),
                0, initialConfig);
        boolean opened = false;
        try {
            // Every node gets its own jitter: with one seed for all, the election timers of
            // all nodes would fire together, election after election.
            RaftConfig config = options.raft().withNode(identity.nodeId(),
                    options.raft().seed() ^ (identity.nodeId() + 1) * 0x9E3779B97F4A7C15L);
            long applied = stateMachine.appliedIndex();
            long damagedFrom = log.firstDamagedIndex();
            // With a damaged log the state machine may be ahead of what the log still holds.
            // The core accepts that only in log-recovering mode, which the report of the
            // damage below is about to start; the flag is durable before anything else happens.
            HardState stored = hardState.get();
            HardState start = damagedFrom == -1 ? stored : new HardState(stored.term(), stored.votedFor(), true,
                    stored.logEpoch(), stored.lostLogIndex(), stored.lostLogTerm());
            RaftNode raft = RaftNode.create(config, new PersistedState(start, SnapshotMeta.none(initialConfig)),
                    log, applied);
            Node node = new Node(identity, hardState, log, raft, stateMachine, transport, listener,
                    options.queueCapacity());
            if (damagedFrom != -1) {
                raft.step(new RaftEvent.LogDamaged(damagedFrom));
                node.drain();
                log.quarantineDamaged();
            }
            opened = true;
            return node;
        } finally {
            if (!opened) {
                log.close();
            }
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
        MessageCodec.Envelope envelope;
        try {
            envelope = MessageCodec.decode(message);
        } catch (DecodeException e) {
            invalidMessages++;
            return false;
        }
        if (envelope.to() != identity.nodeId()) {
            invalidMessages++;
            return false;
        }
        return enqueue(new RaftEvent.MessageReceived(envelope.from(), envelope.message()));
    }

    /** Hands an event of a client or an administrator to the core. */
    public boolean submit(RaftEvent event) {
        return enqueue(event);
    }

    private boolean enqueue(RaftEvent event) {
        if (stopped || queue.size() >= queueCapacity) {
            rejectedEvents++;
            return false;
        }
        queue.addLast(event);
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
        RaftEvent event = queue.pollFirst();
        if (event == null) {
            return false;
        }
        raft.step(event);
        drain();
        return true;
    }

    /** Does the work the core has ready, until it has none. */
    private void drain() throws IOException {
        try {
            while (raft.hasReady()) {
                Ready ready = raft.takeReady();
                if (ready.hasPersistentWork()) {
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
                    raft.step(new RaftEvent.Persisted(ready.sequence()));
                }
                for (Outbound outbound : ready.messages()) {
                    transport.send(outbound.toNode(),
                            MessageCodec.encode(identity.nodeId(), outbound.toNode(), outbound.message()));
                }
                if (ready.hasEntriesToApply()) {
                    List<LogEntry> entries = log.entries(ready.applyFrom(), ready.applyTo() + 1, Integer.MAX_VALUE);
                    List<CommandResult> results = stateMachine.apply(entries);
                    for (int i = 0; i < entries.size(); i++) {
                        listener.applied(entries.get(i), results.get(i));
                    }
                }
                for (Notification note : ready.notifications()) {
                    listener.notification(note);
                }
            }
        } catch (IOException | RuntimeException e) {
            // Whatever failed, the state in memory may be ahead of the state on disk.
            stopped = true;
            queue.clear();
            throw e;
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

    @Override
    public void close() throws IOException {
        stopped = true;
        queue.clear();
        log.close();
    }
}
