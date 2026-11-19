package io.github.sz.freyja.node;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;

import io.github.sz.freyja.common.Clock;
import io.github.sz.freyja.common.Metrics;
import io.github.sz.freyja.common.kv.Command;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.node.service.AdminService;
import io.github.sz.freyja.node.service.ClientService;
import io.github.sz.freyja.node.sm.KvStateMachine;
import io.github.sz.freyja.node.sm.StateMachineStore;
import io.github.sz.freyja.node.store.LostIdentityException;
import io.github.sz.freyja.raft.PeerProgress;
import io.github.sz.freyja.raft.ProtocolState;

/**
 * Runs one node in real time (docs/spec/node.md, section 1): the thread that steps the event
 * loop, driven by the clock.
 *
 * <p>The thread reports ticks as time passes, hands over what the network delivers, lets the
 * node work until it is idle, and lets the service answer the clients. Frames of peers,
 * requests of clients, and calls of an administrator reach it through one bounded queue; a
 * producer never waits, and what does not fit is refused.
 *
 * <p>A node that stops after a failure of storage, or to give up a damaged state machine, is
 * started again from what is on disk, after a pause. A node that has lost its identity is not
 * started again: the runtime ends, and the member must be replaced.
 */
public final class NodeRuntime implements Closeable {

    /**
     * @param dataDirectory   the prepared data directory of the node
     * @param node            the settings of the node; its clock is the one of the runtime
     * @param machine         the settings of the state machine
     * @param service         the settings of the service for clients
     * @param tickMillis      how long a tick takes
     * @param inboundCapacity how many frames, requests, and calls may wait for the thread
     * @param restartMillis   the pause before a stopped node is started again
     * @param wallClockMillis the time of day; commands are stamped with it
     * @param clientAddress   the address for clients of a node, by its id; empty if not known
     * @param addresses       is told where a node listens whenever the replicated state says
     *                        so anew, on the thread of the node; {@code null} if nobody cares
     */
    public record Config(Path dataDirectory, Node.Options node, StateMachineStore.Options machine,
                         ClientService.Options service, int tickMillis, int inboundCapacity, int restartMillis,
                         LongSupplier wallClockMillis, IntFunction<String> clientAddress,
                         BiConsumer<Integer, KvStateMachine.NodeAddress> addresses) {
        public Config {
            if (tickMillis < 1 || inboundCapacity < 1 || restartMillis < 0) {
                throw new IllegalArgumentException("tick " + tickMillis + " ms, inbound " + inboundCapacity
                        + ", restart after " + restartMillis + " ms");
            }
        }
    }

    private static final System.Logger LOG = System.getLogger("freyja.node");
    /** A node this many entries behind what is committed does not count as ready. */
    private static final long READY_LAG = 10_000;

    /** A request of a client on its way to the thread of the node. */
    private record ClientCall(byte[] frame, ClientService.Reply reply) {
    }

    /** A request of an administrator on its way to the thread of the node. */
    private record AdminCall(byte[] frame, ClientService.Reply reply) {
    }

    /** A call that runs on the thread of the node. */
    private record NodeCall<T>(Function<Node, T> call, CompletableFuture<T> result) {
        void run(Node node) {
            try {
                result.complete(call.apply(node));
            } catch (RuntimeException e) {
                result.completeExceptionally(e);
            }
        }
    }

    private final StorageFileSystem fs;
    private final Config config;
    private final Clock clock;
    private final ClientService service;
    private final AdminService admin = new AdminService();
    /** The state machine whose addresses were passed on last, and how many changes it had then. */
    private KvStateMachine addressesOf;
    private long addressChanges = -1;
    private final ArrayBlockingQueue<Object> inbound;
    private Transport transport;
    private Thread thread;
    private volatile boolean closed;
    private volatile boolean running;
    private volatile Throwable fatal;
    private volatile Throwable lastFailure;
    private volatile long restarts;
    private volatile String notReady = "the node is not running";
    private Metrics metrics = Metrics.NONE;
    private long publishes;
    private String loggedRole;
    private String loggedMembers;
    private boolean loggedRecovering;
    private boolean loggedAlarm;
    private long loggedInstalled;
    private final AtomicLong refused = new AtomicLong();

    /** Only the thread of the runtime touches the node. */
    private Node node;

    public NodeRuntime(StorageFileSystem fs, Config config) {
        this.fs = fs;
        this.config = config;
        this.clock = config.node().clock();
        this.service = new ClientService(config.service(), config.wallClockMillis(), config.clientAddress(), admin);
        this.inbound = new ArrayBlockingQueue<>(config.inboundCapacity());
    }

    /**
     * Starts the thread. The transport is given here and not to the constructor, because it
     * delivers to {@link #peerFrame} of this runtime.
     */
    public synchronized void start(Transport transport) {
        if (thread != null) {
            throw new IllegalStateException("the runtime was started already");
        }
        this.transport = transport;
        thread = Thread.ofPlatform().name("freyja-node").start(this::run);
    }

    // ------------------------------------------------------------------ input, from any thread

    /** A frame of a peer has arrived. A frame that does not fit is dropped. */
    public void peerFrame(byte[] frame) {
        if (!inbound.offer(frame)) {
            refused.incrementAndGet();
        }
    }

    /**
     * A request of a client has arrived.
     *
     * @return {@code false} if it could not be taken: the node has too much to do
     */
    public boolean clientRequest(byte[] frame, ClientService.Reply reply) {
        if (inbound.offer(new ClientCall(frame, reply))) {
            return true;
        }
        refused.incrementAndGet();
        return false;
    }

    /**
     * A request of an administrator has arrived.
     *
     * @return {@code false} if it could not be taken: the node has too much to do
     */
    public boolean adminRequest(byte[] frame, ClientService.Reply reply) {
        if (inbound.offer(new AdminCall(frame, reply))) {
            return true;
        }
        refused.incrementAndGet();
        return false;
    }

    /**
     * Runs {@code call} on the thread of the node, with the node. The result is failed if the
     * queue is full or the node is not running.
     */
    public <T> CompletableFuture<T> onNode(Function<Node, T> call) {
        CompletableFuture<T> result = new CompletableFuture<>();
        if (!inbound.offer(new NodeCall<>(call, result))) {
            refused.incrementAndGet();
            result.completeExceptionally(new IllegalStateException("the node has too much to do"));
        }
        return result;
    }

    // ------------------------------------------------------------------ the thread

    private void run() {
        long tickNanos = TimeUnit.MILLISECONDS.toNanos(config.tickMillis());
        long nextTick = clock.nanoTime() + tickNanos;
        while (!closed) {
            try {
                if (node == null) {
                    open();
                    nextTick = clock.nanoTime() + tickNanos;
                }
                long now = clock.nanoTime();
                boolean ticked = false;
                for (int ticks = 0; now - nextTick >= 0 && ticks < 5; ticks++) {
                    node.tick();
                    nextTick += tickNanos;
                    ticked = true;
                }
                if (now - nextTick >= 0) {
                    nextTick = now + tickNanos; // far behind: time that was missed is not made up
                }
                Object item = inbound.poll();
                for (int taken = 0; item != null; item = taken < 256 ? inbound.poll() : null) {
                    dispatch(item);
                    taken++;
                }
                while (node.runOnce()) {
                    // work the queue of the node off
                }
                service.afterStep();
                passAddressesOn();
                if (ticked) {
                    publish();
                }
                if (inbound.isEmpty()) {
                    long wait = nextTick - clock.nanoTime();
                    if (wait > 0) {
                        Object next = inbound.poll(wait, TimeUnit.NANOSECONDS);
                        if (next != null) {
                            dispatch(next);
                        }
                    }
                }
            } catch (InterruptedException e) {
                break;
            } catch (LostIdentityException e) {
                fatal = e;
                LOG.log(System.Logger.Level.ERROR, "the node has lost its identity and does not run again;"
                        + " replace the member: " + e.getMessage());
                break;
            } catch (IOException | RuntimeException e) {
                // The node has stopped. It is started again from what is on disk.
                lastFailure = e;
                restarts++;
                LOG.log(System.Logger.Level.ERROR, "the node stopped and is started again: " + e, e);
                stopNode();
                try {
                    Thread.sleep(config.restartMillis());
                } catch (InterruptedException interrupted) {
                    break;
                }
            }
        }
        stopNode();
        // Whoever still waits in the queue is told that nothing will come.
        for (Object item = inbound.poll(); item != null; item = inbound.poll()) {
            if (item instanceof NodeCall<?> call) {
                call.result().completeExceptionally(new IllegalStateException("the node is not running"));
            }
        }
    }

    private void open() throws IOException {
        node = Node.open(fs, config.dataDirectory(), config.node(), config.machine(), transport, service);
        service.attach(node);
        admin.attach(node, restarts);
        running = true;
        LOG.log(System.Logger.Level.INFO, "node " + node.identity().nodeId() + " of cluster "
                + Long.toHexString(node.identity().clusterId()) + " started: " + node.view() + ", applied "
                + node.store().appliedIndex()
                + (node.repairedAtStart() ? "; its state machine was damaged and is being rebuilt" : "")
                + (node.lostTailWitnessed() ? "; its log had lost entries that were synced" : ""));
    }

    /**
     * Publishes the state of the node as gauges, notes whether it can serve, and logs every
     * change of its state once (docs/spec/operations.md, section 2). Once per tick.
     */
    private void publish() {
        ProtocolState state = node.view();
        KvStateMachine machine = node.store().machine();
        long applied = machine.appliedIndex();
        boolean alarm = machine.alarms().contains(Command.Alarm.NOSPACE);
        metrics.gauge("raft.term", state.term());
        metrics.gauge("raft.role", state.role().ordinal());
        metrics.gauge("raft.leader", state.leaderId());
        metrics.gauge("raft.last.index", state.lastLogIndex());
        metrics.gauge("raft.commit.index", state.commitIndex());
        metrics.gauge("raft.applied.index", applied);
        metrics.gauge("raft.snapshot.index", node.snapshot().lastIncludedIndex());
        metrics.gauge("raft.log.recovering", state.logRecovering() ? 1 : 0);
        metrics.gauge("raft.log.segments", node.logSegmentCount());
        metrics.gauge("node.stored.bytes", node.logStoredBytes() + machine.engine().storedBytes());
        metrics.gauge("node.queue.events", node.waitingEvents());
        metrics.gauge("node.queue.inbound", inbound.size());
        metrics.gauge("node.events.rejected", node.rejectedEvents());
        metrics.gauge("node.messages.invalid", node.invalidMessages());
        metrics.gauge("node.restarts", restarts);
        metrics.gauge("node.inbound.refused", refused.get());
        metrics.gauge("node.snapshots.created", node.snapshotsCreated());
        metrics.gauge("node.snapshots.installed", node.snapshotsInstalled());
        metrics.gauge("node.proposals.expired", node.expiredProposals());
        metrics.gauge("node.slow.sync.step.downs", node.slowSyncStepDowns());
        metrics.gauge("transfers.outgoing", node.transfers().outgoingCount());
        metrics.gauge("transfers.sent", node.transfers().sentCount());
        metrics.gauge("transfers.received", node.transfers().receivedCount());
        metrics.gauge("transfers.failed", node.transfers().failedCount());
        metrics.gauge("client.waiting", service.waitingCount());
        metrics.gauge("client.dropped", service.droppedCount());
        metrics.gauge("alarm.nospace", alarm ? 1 : 0);
        metrics.gauge("cluster.version", machine.clusterVersion());
        metrics.gauge("sessions", machine.sessionCount());
        for (PeerProgress peer : node.peers()) {
            metrics.gauge("raft.follower." + peer.nodeId() + ".lag", state.lastLogIndex() - peer.matchIndex());
            metrics.gauge("raft.follower." + peer.nodeId() + ".active", peer.active() ? 1 : 0);
        }
        if (++publishes % 50 == 0) {
            machine.engine().reportGauges();
        }

        String waitingFor = state.logRecovering() ? "the node is refilling its log"
                : state.leaderId() < 0 ? "the node knows no leader"
                : state.commitIndex() - applied > READY_LAG ? "the node is " + (state.commitIndex() - applied)
                + " entries behind what is committed" : null;
        notReady = waitingFor;

        String described = state.role() + " in term " + state.term() + ", leader "
                + (state.leaderId() < 0 ? "unknown" : "node " + state.leaderId());
        if (!described.equals(loggedRole)) {
            if (state.role().name().equals("LEADER") && !described.equals(loggedRole)) {
                metrics.count("raft.elections.won", 1);
            }
            loggedRole = described;
            LOG.log(System.Logger.Level.INFO, "node " + state.nodeId() + " is " + described);
        }
        String members = "voters " + new java.util.TreeSet<>(state.config().voters()) + ", learners "
                + new java.util.TreeSet<>(state.config().learners());
        if (!members.equals(loggedMembers)) {
            loggedMembers = members;
            LOG.log(System.Logger.Level.INFO, "node " + state.nodeId() + ": the configuration in force is "
                    + members);
        }
        if (state.logRecovering() != loggedRecovering) {
            loggedRecovering = state.logRecovering();
            LOG.log(System.Logger.Level.WARNING, "node " + state.nodeId() + (loggedRecovering
                    ? " has lost entries of its log and is refilling it from the leader"
                    : " has its log back and counts again"));
        }
        if (alarm != loggedAlarm) {
            loggedAlarm = alarm;
            LOG.log(System.Logger.Level.WARNING, "the alarm for lack of space is " + (alarm
                    ? "set: only deletes are accepted" : "cleared"));
        }
        long installed = node.snapshotsInstalled();
        if (installed != loggedInstalled) {
            loggedInstalled = installed;
            LOG.log(System.Logger.Level.INFO, "node " + state.nodeId() + " took the state machine of the leader"
                    + " through index " + node.snapshot().lastIncludedIndex());
        }
    }

    /** Tells the owner of the network where the nodes listen, when the replicated state has news. */
    private void passAddressesOn() {
        if (config.addresses() == null) {
            return;
        }
        KvStateMachine machine = node.store().machine();
        if (machine != addressesOf || machine.addressChanges() != addressChanges) {
            addressesOf = machine;
            addressChanges = machine.addressChanges();
            machine.addresses().forEach(config.addresses());
        }
    }

    private void stopNode() {
        running = false;
        notReady = "the node is not running";
        loggedInstalled = 0;
        service.detach();
        admin.detach();
        if (node != null) {
            try {
                node.close();
            } catch (IOException | RuntimeException e) {
                lastFailure = e;
            }
            node = null;
        }
    }

    private void dispatch(Object item) throws IOException {
        switch (item) {
            case byte[] frame -> node.offer(frame);
            case ClientCall call -> service.request(call.frame(), call.reply());
            case AdminCall call -> admin.request(call.frame(), call.reply());
            case NodeCall<?> call -> call.run(node);
            default -> throw new IllegalStateException("item " + item.getClass());
        }
    }

    // ------------------------------------------------------------------ status, from any thread

    /**
     * Where the runtime, the service for clients, and the storage engine report. Set before
     * {@link #start}; the engine settings of the configuration must name the same sink.
     */
    public void measure(Metrics metrics) {
        this.metrics = metrics;
        service.measure(metrics, clock);
    }

    /** Why the node cannot serve now, or {@code null} if it can. For the readiness endpoint. */
    public String notReady() {
        return notReady;
    }

    /** Why the runtime does not run its node, or {@code null} if it does. For the liveness endpoint. */
    public String notAlive() {
        Throwable ended = fatal;
        Thread running;
        synchronized (this) {
            running = thread;
        }
        return ended != null ? "the node has ended for good: " + ended.getMessage()
                : running == null || !running.isAlive() ? "the thread of the node is not running" : null;
    }

    /** Whether the node is open and being stepped. */
    public boolean running() {
        return running;
    }

    /** Why the runtime has ended for good, or {@code null}: the node has lost its identity. */
    public Throwable fatal() {
        return fatal;
    }

    /** The failure that stopped the node last, or {@code null}. */
    public Throwable lastFailure() {
        return lastFailure;
    }

    /** How often a stopped node was started again. */
    public long restarts() {
        return restarts;
    }

    /** Frames, requests, and calls that did not fit into the queue. */
    public long refused() {
        return refused.get();
    }

    /** Stops the thread and closes the node. */
    @Override
    public void close() {
        closed = true;
        Thread running;
        synchronized (this) {
            running = thread;
        }
        if (running != null) {
            // No interrupt: it would close files that the node is in the middle of using.
            // The thread looks at the flag at least once per tick.
            try {
                running.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
