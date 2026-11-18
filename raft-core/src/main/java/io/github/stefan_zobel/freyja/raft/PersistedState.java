package io.github.stefan_zobel.freyja.raft;

import java.util.Objects;

/**
 * What the runtime read from stable storage before it creates a node.
 *
 * <p>The configuration in force is not part of it: the node derives it from the configuration
 * of the snapshot and the CONFIG entries of the stored log.
 *
 * @param hardState the stored hard state
 * @param snapshot  the newest snapshot; without one, the configuration the cluster started with
 */
public record PersistedState(HardState hardState, SnapshotMeta snapshot) {

    public PersistedState {
        Objects.requireNonNull(hardState, "hardState");
        Objects.requireNonNull(snapshot, "snapshot");
    }

    /** The state of a node that starts for the first time as a member of {@code config}. */
    public static PersistedState initial(ClusterConfig config) {
        return new PersistedState(HardState.INITIAL, SnapshotMeta.none(config));
    }
}
