package io.github.stefan_zobel.freyja.raft;

/**
 * The part of the node state that must be durable before any message that depends on it is
 * sent.
 *
 * @param term          the current term
 * @param votedFor      the node this node voted for in {@code term}, or {@link #NO_VOTE}
 * @param logRecovering whether the node is in log-recovering mode
 */
public record HardState(long term, int votedFor, boolean logRecovering) {

    /** The value of {@link #votedFor} when no vote was cast in the term. */
    public static final int NO_VOTE = -1;

    /** The hard state of a node that has never run. */
    public static final HardState INITIAL = new HardState(0, NO_VOTE, false);

    public HardState {
        if (term < 0 || votedFor < NO_VOTE) {
            throw new IllegalArgumentException("term " + term + ", votedFor " + votedFor);
        }
    }
}
