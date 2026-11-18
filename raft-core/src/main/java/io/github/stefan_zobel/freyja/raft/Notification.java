package io.github.stefan_zobel.freyja.raft;

/** What the core tells the runtime besides the work of a {@link Ready}. */
public sealed interface Notification {

    /**
     * The proposal was appended to the log of the leader. It took effect if and only if the
     * entry at {@code index} is applied and has the term {@code term}.
     */
    record ProposalAccepted(long requestId, long index, long term) implements Notification {
    }

    /**
     * The proposal was not appended.
     *
     * @param leaderHint the leader this node knows of, or {@link RaftNode#NO_NODE}
     */
    record ProposalRejected(long requestId, Reason reason, int leaderHint) implements Notification {
    }

    /**
     * Leadership was confirmed after the request arrived. A read served from the state
     * machine of this node after it has applied everything through {@code index} is
     * linearizable.
     */
    record ReadIndexReady(long requestId, long index) implements Notification {
    }

    /**
     * No read index could be given.
     *
     * @param leaderHint the leader this node knows of, or {@link RaftNode#NO_NODE}
     */
    record ReadIndexRejected(long requestId, Reason reason, int leaderHint) implements Notification {
    }

    /**
     * Another configuration is in force on this node: one arrived in the log, or the entry
     * that carried the previous one was removed from it.
     *
     * @param index the index of the entry that carries the configuration
     */
    record ConfigChanged(long index, ClusterConfig config) implements Notification {
    }

    /**
     * A leadership transfer did not succeed within one election timeout. The leader accepts
     * proposals again.
     */
    record TransferAborted(long requestId, int toNode) implements Notification {
    }

    /** The role, the term, or the known leader of this node changed. */
    record RoleChanged(Role role, long term, int leaderId) implements Notification {
    }

    /** Why a request was rejected. */
    enum Reason {
        /** This node is not the leader, or it stopped being the leader before it could answer. */
        NOT_LEADER,
        /** The uncommitted part of the log of the leader is at its limit. */
        OVERLOADED,
        /** The leader has not yet committed an entry of its own term. */
        NOT_READY,
        /** An earlier membership change is not committed yet. */
        CONFIG_IN_PROGRESS,
        /** The change does not fit the configuration in force. */
        INVALID_CHANGE,
        /** The learner to promote is too far behind the log of the leader. */
        LEARNER_BEHIND,
        /** The leader is handing leadership to another node and accepts nothing meanwhile. */
        TRANSFER_IN_PROGRESS
    }
}
