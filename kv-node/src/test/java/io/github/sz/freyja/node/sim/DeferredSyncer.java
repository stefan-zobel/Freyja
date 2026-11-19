package io.github.sz.freyja.node.sim;

import io.github.sz.freyja.node.LogSyncer;

/**
 * Runs the syncs of the log of a simulated node on the thread of the simulation, at a moment
 * the simulation picks: a sync that is started waits until it is released, and runs when the
 * node next asks whether it has ended. Until then the node goes on as it would while a real
 * sync takes its time, and a crash finds the appended entries not durable.
 */
final class DeferredSyncer implements LogSyncer {

    private Runnable pending;
    private boolean released;

    @Override
    public void start(Runnable sync) {
        if (pending != null) {
            throw new IllegalStateException("a sync is running already");
        }
        pending = sync;
        released = false;
    }

    @Override
    public boolean done() {
        if (pending != null && released) {
            run();
        }
        return pending == null;
    }

    @Override
    public void await() {
        if (pending != null) {
            run();
        }
    }

    /** Whether a sync was started that has not run yet. */
    boolean pending() {
        return pending != null;
    }

    /** Lets the sync that waits run when the node next asks for it. */
    void release() {
        released = true;
    }

    private void run() {
        Runnable sync = pending;
        pending = null;
        sync.run();
    }
}
