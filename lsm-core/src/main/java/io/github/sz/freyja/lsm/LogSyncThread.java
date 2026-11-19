package io.github.sz.freyja.lsm;

import java.io.IOException;
import java.util.function.Consumer;

import io.github.sz.freyja.common.Clock;
import io.github.sz.freyja.lsm.log.LogWriter;

/**
 * A thread that syncs a log while writers go on appending to it, one sync at a time.
 *
 * <p>The writer at the head of the queue starts a sync with the size of the log at that
 * moment, and later takes note of it ({@link LogWriter#synced}) when the sync is done. Before a
 * log is sealed, cut back, or closed, the sync that runs on it is awaited.
 */
final class LogSyncThread {

    /** One sync of a log. Its fields are read once {@link #done} is true. */
    static final class Sync {
        final LogWriter writer;
        final long length;
        private long nanos;
        private IOException failure;
        private volatile boolean done;

        private Sync(LogWriter writer, long length) {
            this.writer = writer;
            this.length = length;
        }

        /** Whether the sync has ended, well or not. Never waits. */
        boolean done() {
            return done;
        }

        long nanos() {
            return nanos;
        }

        /** Why the sync failed, or {@code null}. */
        IOException failure() {
            return failure;
        }
    }

    private final Clock clock;
    private final Consumer<IOException> onFailure;
    private final Thread thread;
    private Sync next;      // guarded by this
    private boolean closed; // guarded by this

    /**
     * @param onFailure told at once, on this thread, when a sync fails
     */
    LogSyncThread(String name, Clock clock, Consumer<IOException> onFailure) {
        this.clock = clock;
        this.onFailure = onFailure;
        this.thread = Thread.ofPlatform().daemon(true).name(name).start(this::loop);
    }

    /**
     * Starts syncing the first {@code length} bytes of {@code writer}.
     *
     * @throws IllegalStateException if a sync is waiting to run or the thread is closed
     */
    synchronized Sync start(LogWriter writer, long length) {
        if (closed || next != null) {
            throw new IllegalStateException("a log sync is pending or the thread is closed");
        }
        next = new Sync(writer, length);
        notifyAll();
        return next;
    }

    /** Waits until {@code sync} has ended. */
    void await(Sync sync) {
        if (sync.done) {
            return;
        }
        boolean interrupted = false;
        synchronized (this) {
            while (!sync.done) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Stops the thread after the sync that runs or waits, if any. */
    void close() {
        synchronized (this) {
            closed = true;
            notifyAll();
        }
        boolean interrupted = false;
        while (true) {
            try {
                thread.join();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void loop() {
        while (true) {
            Sync sync;
            synchronized (this) {
                while (next == null && !closed) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        // Only a close ends this thread.
                    }
                }
                if (next == null) {
                    return;
                }
                sync = next;
            }
            long started = clock.nanoTime();
            try {
                sync.writer.syncFile();
            } catch (IOException e) {
                sync.failure = e;
            } catch (RuntimeException e) {
                sync.failure = new IOException("a sync of the log failed", e);
            }
            sync.nanos = clock.nanoTime() - started;
            synchronized (this) {
                sync.done = true;
                next = null;
                notifyAll();
            }
            if (sync.failure != null) {
                onFailure.accept(sync.failure);
            }
        }
    }
}
