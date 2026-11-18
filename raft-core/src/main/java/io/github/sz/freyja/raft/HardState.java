package io.github.sz.freyja.raft;

/**
 * The part of the node state that must be durable before any message that depends on it is
 * sent.
 *
 * @param term          the current term
 * @param votedFor      the node this node voted for in {@code term}, or {@link #NO_VOTE}
 * @param logRecovering whether the node is in log-recovering mode
 * @param logEpoch      how often the node has lost entries of its log; what it said about its
 *                      log in an earlier epoch no longer holds
 * @param lostLogIndex  in log-recovering mode: the last index of the log before it was
 *                      damaged, if known
 * @param lostLogTerm   in log-recovering mode: the term of that entry, or
 *                      {@link #UNKNOWN_TERM} if the log before the damage is not known. The
 *                      node votes only for a candidate whose log is at least as up to date
 *                      as the log it has lost.
 */
public record HardState(long term, int votedFor, boolean logRecovering, long logEpoch,
                        long lostLogIndex, long lostLogTerm) {

    /** The term of a lost log that is not known: no candidate is as up to date. */
    public static final long UNKNOWN_TERM = Long.MAX_VALUE;

    /** The hard state of a node that has never lost entries of its log. */
    public HardState(long term, int votedFor, boolean logRecovering) {
        this(term, votedFor, logRecovering, 0, 0, 0);
    }

    /** The hard state of a node whose log is intact now. */
    public HardState(long term, int votedFor, boolean logRecovering, long logEpoch) {
        this(term, votedFor, logRecovering, logEpoch, 0, 0);
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
