package io.github.sz.freyja.raft;

import java.util.List;

/**
 * The messages nodes exchange. Every message carries the term of its sender; the sender and
 * the receiver travel next to the message ({@link Outbound}, {@link RaftEvent.MessageReceived}).
 */
public sealed interface RaftMessage {

    long term();

    /**
     * Asks for a vote.
     *
     * @param term         the term of the election. For a pre-vote this is the term the sender
     *                     would enter, one above its current term.
     * @param lastLogIndex index of the last log entry of the sender
     * @param lastLogTerm  term of that entry
     * @param preVote      whether this only asks if a vote would be granted
     * @param transfer     whether the election is part of a leadership transfer: the leader
     *                     itself asked the sender to start it, so the vote is to be granted
     *                     although a leader was heard from recently
     */
    record RequestVote(long term, long lastLogIndex, long lastLogTerm, boolean preVote,
                       boolean transfer) implements RaftMessage {

        /** A request that is not part of a leadership transfer. */
        public RequestVote(long term, long lastLogIndex, long lastLogTerm, boolean preVote) {
            this(term, lastLogIndex, lastLogTerm, preVote, false);
        }
    }

    /**
     * Answers a {@link RequestVote}.
     *
     * @param term    the term of the election when granted, otherwise the term of the sender
     * @param preVote whether this answers a pre-vote
     * @param granted whether the vote is (or would be) granted
     */
    record VoteResponse(long term, boolean preVote, boolean granted) implements RaftMessage {
    }

    /**
     * Replicates entries from the leader.
     *
     * @param prevLogIndex index of the entry before {@code entries}
     * @param prevLogTerm  term of that entry
     * @param entries      the entries, possibly none
     * @param leaderCommit commit index of the leader
     * @param followerEpoch the log epoch of the receiver as the leader knows it, or -1 if the
     *                     leader has not heard from the receiver yet. A node in log-recovering
     *                     mode leaves that mode only on an append that names its present
     *                     epoch: such an append was sent after the leader had dropped what it
     *                     knew about the log the node has lost.
     */
    record AppendEntries(long term, long prevLogIndex, long prevLogTerm, List<LogEntry> entries,
                         long leaderCommit, long followerEpoch) implements RaftMessage {
        public AppendEntries {
            entries = List.copyOf(entries);
        }

        /** An append of a leader that has not heard from the receiver yet. */
        public AppendEntries(long term, long prevLogIndex, long prevLogTerm, List<LogEntry> entries,
                             long leaderCommit) {
            this(term, prevLogIndex, prevLogTerm, entries, leaderCommit, -1);
        }
    }

    /**
     * Answers an {@link AppendEntries}.
     *
     * @param success   whether the log of the sender now matches the leader through
     *                  {@code index}
     * @param index     on success the last index known to match and to be durable; otherwise
     *                  the {@code prevLogIndex} that was rejected
     * @param hintIndex on rejection: the leader should try again at or below this index
     * @param hintTerm  on rejection: the term of the sender at {@code hintIndex}
     * @param logEpoch  the log epoch of the sender. An answer of an earlier epoch than the
     *                  leader has seen from the sender says nothing about its present log.
     */
    record AppendResponse(long term, boolean success, long index, long hintIndex, long hintTerm,
                          long logEpoch) implements RaftMessage {

        /** An answer of a node that has never lost entries of its log. */
        public AppendResponse(long term, boolean success, long index, long hintIndex, long hintTerm) {
            this(term, success, index, hintIndex, hintTerm, 0);
        }
    }

    /**
     * Asserts leadership between appends.
     *
     * @param commit the commit index, limited to what the receiver is known to hold
     * @param round  number of the heartbeat round; echoed in the response
     */
    record Heartbeat(long term, long commit, long round) implements RaftMessage {
    }

    /**
     * Leadership transfer: the leader tells an up-to-date voter to start an election at
     * once, without a pre-vote.
     */
    record TimeoutNow(long term) implements RaftMessage {
    }

    /**
     * Answers a {@link Heartbeat}.
     *
     * @param recovering whether the sender is in log-recovering mode. The leader then sends
     *                   an append even if it has nothing new: the node leaves the mode only
     *                   on an append.
     */
    record HeartbeatResponse(long term, long round, boolean recovering) implements RaftMessage {

        /** An answer of a node that is not in log-recovering mode. */
        public HeartbeatResponse(long term, long round) {
            this(term, round, false);
        }
    }
}
