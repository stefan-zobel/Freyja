package io.github.sz.freyja.node;

import java.io.Closeable;

/**
 * Runs the syncs of the Raft log of a node on a platform thread of their own. When a sync has
 * ended, the thread of the node is woken up so that it hears of it at once.
 */
public final class SyncThread implements LogSyncer, Closeable {

    private final Runnable wake;
    private final Thread thread;
    private Runnable task;
    private boolean running;
    private boolean closed;

    /**
     * @param wake called on the sync thread after every sync; it must not wait
     */
    public SyncThread(String name, Runnable wake) {
        this.wake = wake;
        this.thread = Thread.ofPlatform().name(name).daemon(true).unstarted(this::loop);
        thread.start();
    }

    @Override
    public synchronized void start(Runnable sync) {
        if (running) {
            throw new IllegalStateException("a sync is running already");
        }
        if (closed) {
            throw new IllegalStateException("the sync thread is closed");
        }
        task = sync;
        running = true;
        notifyAll();
    }

    @Override
    public synchronized boolean done() {
        return !running;
    }

    @Override
    public synchronized void await() {
        boolean interrupted = false;
        while (running) {
            try {
                wait();
            } catch (InterruptedException e) {
                // A sync cannot be left half done: whoever waits for it waits on.
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void loop() {
        while (true) {
            Runnable next;
            synchronized (this) {
                while (task == null && !closed) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        // only close ends the thread
                    }
                }
                if (task == null) {
                    return;
                }
                next = task;
                task = null;
            }
            try {
                next.run();
            } finally {
                synchronized (this) {
                    running = false;
                    notifyAll();
                }
                wake.run();
            }
        }
    }

    /** Ends the thread after the sync that runs, if any. */
    @Override
    public void close() {
        synchronized (this) {
            closed = true;
            notifyAll();
        }
        boolean interrupted = false;
        while (thread.isAlive()) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
