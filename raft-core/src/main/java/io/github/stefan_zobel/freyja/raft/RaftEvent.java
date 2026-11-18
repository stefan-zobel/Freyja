package io.github.stefan_zobel.freyja.raft;

import java.util.Objects;

/** Everything that can happen to a node. Events are the only input of the core. */
public sealed interface RaftEvent {

    /** One unit of time has passed. All timeouts are counted in ticks. */
    record Tick() implements RaftEvent {
    }

    /** A message from another node arrived. */
    record MessageReceived(int fromNode, RaftMessage message) implements RaftEvent {
        public MessageReceived {
            Objects.requireNonNull(message, "message");
        }
    }

    /**
     * Everything the {@link Ready}s up to and including {@code readySequence} asked to persist
     * is durable.
     */
    record Persisted(long readySequence) implements RaftEvent {
    }

    /**
     * A client asks for an index at which a read is linearizable. The read may be served
     * from the state machine of this node once it has applied everything through that index.
     */
    record ClientReadIndex(long requestId) implements RaftEvent {
    }

    /** An administrator asks to change the membership of the cluster. */
    record ProposeConfigChange(long requestId, ConfigChange change) implements RaftEvent {
        public ProposeConfigChange {
            Objects.requireNonNull(change, "change");
        }
    }

    /** An administrator asks the leader to hand leadership to another voter. */
    record TransferLeadership(long requestId, int toNode) implements RaftEvent {
    }

    /**
     * The runtime found that stored log entries from {@code firstDamagedIndex} on cannot be
     * read. The node may have acknowledged them; it enters log-recovering mode.
     */
    record LogDamaged(long firstDamagedIndex) implements RaftEvent {
    }

    /**
     * The state machine has written a snapshot. The log up to its last included index is no
     * longer needed; the next {@link Ready} says so ({@code truncatePrefixThrough}).
     *
     * @param meta as {@link RaftNode#snapshotMeta} gave it for the index of the snapshot
     */
    record SnapshotCreated(SnapshotMeta meta) implements RaftEvent {
    }

    /** The runtime has received a complete snapshot from a leader and holds it staged. */
    record SnapshotOffered(int fromNode, long leaderTerm, SnapshotMeta meta) implements RaftEvent {
    }

    /** The snapshot an {@link SnapshotDirective.Install} named is installed and durable. */
    record SnapshotInstalled(SnapshotMeta meta) implements RaftEvent {
    }

    /** The stream a {@link SnapshotDirective.Send} asked for has ended. */
    record SnapshotSendFinished(int toNode, boolean success) implements RaftEvent {
    }

    /** A client asks to append a command to the log. */
    record ClientPropose(long requestId, byte[] command) implements RaftEvent {
        public ClientPropose {
            Objects.requireNonNull(command, "command");
        }
    }
}
