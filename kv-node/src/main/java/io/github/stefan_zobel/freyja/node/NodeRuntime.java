package io.github.stefan_zobel.freyja.node;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;

import io.github.stefan_zobel.freyja.common.Clock;
import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;
import io.github.stefan_zobel.freyja.node.service.ClientService;
import io.github.stefan_zobel.freyja.node.sm.StateMachineStore;
import io.github.stefan_zobel.freyja.node.store.LostIdentityException;

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
     */
    public record Config(Path dataDirectory, Node.Options node, StateMachineStore.Options machine,
                         ClientService.Options service, int tickMillis, int inboundCapacity, int restartMillis,
                         LongSupplier wallClockMillis, IntFunction<String> clientAddress) {
        public Config {
            if (tickMillis < 1 || inboundCapacity < 1 || restartMillis < 0) {
                throw new IllegalArgumentException("tick " + tickMillis + " ms, inbound " + inboundCapacity
                        + ", restart after " + restartMillis + " ms");
            }
        }
    }

    /** A request of a client on its way to the thread of the node. */
    private record ClientCall(byte[] frame, ClientService.Reply reply) {
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
    private final ArrayBlockingQueue<Object> inbound;
    private Transport transport;
    private Thread thread;
    private volatile boolean closed;
    private volatile boolean running;
    private volatile Throwable fatal;
    private volatile Throwable lastFailure;
    private volatile long restarts;
    private final AtomicLong refused = new AtomicLong();

    /** Only the thread of the runtime touches the node. */
    private Node node;

    public NodeRuntime(StorageFileSystem fs, Config config) {
        this.fs = fs;
        this.config = config;
        this.clock = config.node().clock();
        this.service = new ClientService(config.service(), config.wallClockMillis(), config.clientAddress(), null);
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
                for (int ticks = 0; now - nextTick >= 0 && ticks < 5; ticks++) {
                    node.tick();
                    nextTick += tickNanos;
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
                break;
            } catch (IOException | RuntimeException e) {
                // The node has stopped. It is started again from what is on disk.
                lastFailure = e;
                restarts++;
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
        running = true;
    }

    private void stopNode() {
        running = false;
        service.detach();
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
            case NodeCall<?> call -> call.run(node);
            default -> throw new IllegalStateException("item " + item.getClass());
        }
    }

    // ------------------------------------------------------------------ status, from any thread

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
