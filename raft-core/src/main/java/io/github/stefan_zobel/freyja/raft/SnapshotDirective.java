package io.github.stefan_zobel.freyja.raft;

/**
 * What the runtime is to do with a snapshot. Snapshots carry no bytes through the core: the
 * runtime moves them, and reports what happened with events.
 */
public sealed interface SnapshotDirective {

    /**
     * Stream the snapshot that {@code meta} describes to {@code toNode}, out of band. Report
     * the outcome with {@link RaftEvent.SnapshotSendFinished}.
     *
     * @param term the term of the leader that sends; the receiving runtime passes it on
     */
    record Send(int toNode, long term, SnapshotMeta meta) implements SnapshotDirective {
    }

    /**
     * Install the staged snapshot: replace the state machine by it and remove from the stored
     * log every entry up to its last included index. Then feed
     * {@link RaftEvent.SnapshotInstalled}.
     *
     * <p>The installation takes its place in the order of the {@link Ready}s: after
     * everything this and earlier {@code Ready}s ask to persist, before anything later.
     *
     * @param keepSuffix whether the stored entries after the last included index stay; if
     *                   not, the stored log is emptied
     * @param replaceStateMachine whether the state machine is replaced. It is not if it has
     *                   already applied everything the snapshot includes: a node whose log
     *                   was damaged can be ahead of its log, and then only its log starts
     *                   anew after the snapshot.
     */
    record Install(int fromNode, SnapshotMeta meta, boolean keepSuffix, boolean replaceStateMachine)
            implements SnapshotDirective {
    }

    /** Discard the staged snapshot: it is out of date or not wanted now. */
    record Reject(int fromNode, SnapshotMeta meta) implements SnapshotDirective {
    }
}
