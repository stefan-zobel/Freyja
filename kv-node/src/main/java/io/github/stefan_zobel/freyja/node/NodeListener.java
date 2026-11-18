package io.github.stefan_zobel.freyja.node;

import io.github.stefan_zobel.freyja.common.kv.CommandResult;
import io.github.stefan_zobel.freyja.raft.LogEntry;
import io.github.stefan_zobel.freyja.raft.HardState;
import io.github.stefan_zobel.freyja.raft.Notification;
import io.github.stefan_zobel.freyja.raft.ProtocolState;
import io.github.stefan_zobel.freyja.raft.RaftEvent;
import io.github.stefan_zobel.freyja.raft.Ready;
import io.github.stefan_zobel.freyja.raft.SnapshotMeta;

/** Is told what happens inside a node. It is called from the event loop and must not block. */
@FunctionalInterface
public interface NodeListener {

    /** The core reports an answer to a proposal or a request, or a change of role. */
    void notification(Notification notification);

    /**
     * An entry was applied to the state machine.
     *
     * @param result the result of its command; {@code null} if the entry carries none
     */
    default void applied(LogEntry entry, CommandResult result) {
    }

    /**
     * Whether the listener wants to see every step of the core ({@link #stepped},
     * {@link #ready}). Asked once, when the node starts. For simulations and for recording
     * traces; a node in service has no use for it.
     */
    default boolean observesCore() {
        return false;
    }

    /** The core has processed an event. Only if {@link #observesCore}. */
    default void stepped(RaftEvent event, ProtocolState state) {
    }

    /**
     * The node has made the work of a {@code Ready} durable and is about to send its
     * messages. Only if {@link #observesCore}.
     *
     * @param durableHardState the hard state in stable storage at this moment
     * @param durableLastIndex the last log index in stable storage at this moment
     */
    default void ready(Ready ready, HardState durableHardState, long durableLastIndex) {
    }

    /**
     * A proposal waited for the event loop beyond its deadline and was dropped. Nobody waits
     * for its answer any more.
     */
    default void proposalExpired(long requestId) {
    }

    /**
     * The state machine was replaced by a snapshot of another node. Whatever the listener
     * derived from applied entries up to the index of the snapshot is out of date.
     */
    default void snapshotInstalled(SnapshotMeta meta) {
    }
}
