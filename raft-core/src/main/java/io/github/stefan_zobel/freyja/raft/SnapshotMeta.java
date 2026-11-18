package io.github.stefan_zobel.freyja.raft;

import java.util.Objects;

/**
 * What a snapshot of the state machine covers.
 *
 * @param lastIncludedIndex index of the last entry the snapshot contains; 0 for no snapshot
 * @param lastIncludedTerm  term of that entry; 0 for no snapshot
 * @param config            the cluster configuration as of that entry
 */
public record SnapshotMeta(long lastIncludedIndex, long lastIncludedTerm, ClusterConfig config) {

    public SnapshotMeta {
        Objects.requireNonNull(config, "config");
        if (lastIncludedIndex < 0 || lastIncludedTerm < 0) {
            throw new IllegalArgumentException(
                    "index " + lastIncludedIndex + ", term " + lastIncludedTerm);
        }
    }

    /** The meta data of a node that has no snapshot. */
    public static SnapshotMeta none(ClusterConfig config) {
        return new SnapshotMeta(0, 0, config);
    }
}
