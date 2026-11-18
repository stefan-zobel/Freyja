package io.github.stefan_zobel.freyja.raft;

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
     */
    record RequestVote(long term, long lastLogIndex, long lastLogTerm, boolean preVote)
            implements RaftMessage {
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
     */
    record AppendEntries(long term, long prevLogIndex, long prevLogTerm, List<LogEntry> entries,
                         long leaderCommit) implements RaftMessage {
        public AppendEntries {
            entries = List.copyOf(entries);
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
     */
    record AppendResponse(long term, boolean success, long index, long hintIndex, long hintTerm)
            implements RaftMessage {
    }

    /**
     * Asserts leadership between appends.
     *
     * @param commit the commit index, limited to what the receiver is known to hold
     * @param round  number of the heartbeat round; echoed in the response
     */
    record Heartbeat(long term, long commit, long round) implements RaftMessage {
    }

    /** Answers a {@link Heartbeat}. */
    record HeartbeatResponse(long term, long round) implements RaftMessage {
    }
}
