package io.github.sz.freyja.node;

/**
 * Runs the syncs of the Raft log away from the event loop (docs/spec/node.md, section 1): while
 * one runs, the node goes on taking events and appending what they produce, and the next sync
 * covers all of that. At most one sync runs at a time.
 *
 * <p>The real runtime runs them on a thread of their own ({@link SyncThread}); a simulation
 * runs them on its own thread at a moment it chooses. A node without one syncs in its loop.
 */
public interface LogSyncer {

    /** Starts running {@code sync}. The sync started before must have ended. */
    void start(Runnable sync);

    /**
     * Whether the sync started last has ended; {@code true} if none was started. Never waits.
     * When it says so, what the sync recorded is visible to the caller.
     */
    boolean done();

    /** Waits until the sync started last has ended. */
    void await();
}
