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

    /** The role, the term, or the known leader of this node changed. */
    record RoleChanged(Role role, long term, int leaderId) implements Notification {
    }

    /** Why a proposal was rejected. */
    enum Reason {
        /** This node is not the leader. */
        NOT_LEADER,
        /** The uncommitted part of the log of the leader is at its limit. */
        OVERLOADED
    }
}
