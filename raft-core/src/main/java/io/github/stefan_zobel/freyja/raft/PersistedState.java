package io.github.stefan_zobel.freyja.raft;

import java.util.Objects;

/**
 * What the runtime read from stable storage before it creates a node.
 *
 * @param hardState the stored hard state
 * @param snapshot  the newest snapshot
 * @param config    the cluster configuration in force at the end of the stored log
 */
public record PersistedState(HardState hardState, SnapshotMeta snapshot, ClusterConfig config) {

    public PersistedState {
        Objects.requireNonNull(hardState, "hardState");
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(config, "config");
    }

    /** The state of a node that starts for the first time as a member of {@code config}. */
    public static PersistedState initial(ClusterConfig config) {
        return new PersistedState(HardState.INITIAL, SnapshotMeta.none(config), config);
    }
}
