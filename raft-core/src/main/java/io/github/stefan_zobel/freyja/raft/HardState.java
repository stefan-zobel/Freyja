package io.github.stefan_zobel.freyja.raft;

/**
 * The part of the node state that must be durable before any message that depends on it is
 * sent.
 *
 * @param term          the current term
 * @param votedFor      the node this node voted for in {@code term}, or {@link #NO_VOTE}
 * @param logRecovering whether the node is in log-recovering mode
 * @param logEpoch      how often the node has lost entries of its log; what it said about its
 *                      log in an earlier epoch no longer holds
 */
public record HardState(long term, int votedFor, boolean logRecovering, long logEpoch) {

    /** The hard state of a node that has never lost entries of its log. */
    public HardState(long term, int votedFor, boolean logRecovering) {
        this(term, votedFor, logRecovering, 0);
    }

    /** The value of {@link #votedFor} when no vote was cast in the term. */
    public static final int NO_VOTE = -1;

    /** The hard state of a node that has never run. */
    public static final HardState INITIAL = new HardState(0, NO_VOTE, false);

    public HardState {
        if (term < 0 || votedFor < NO_VOTE || logEpoch < 0) {
            throw new IllegalArgumentException("term " + term + ", votedFor " + votedFor);
        }
    }
}
