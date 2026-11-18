package io.github.sz.freyja.raft;

/**
 * A read-only view of the protocol state of a node, for checks and for trace recording.
 * Taking it performs no I/O and changes nothing.
 *
 * @param lastLogIndex index of the last entry, durable or not
 * @param lastLogTerm  term of that entry
 * @param durableIndex index of the last entry known to be durable
 */
public record ProtocolState(
        int nodeId,
        long term,
        Role role,
        int votedFor,
        int leaderId,
        long lastLogIndex,
        long lastLogTerm,
        long durableIndex,
        long commitIndex,
        ClusterConfig config,
        boolean logRecovering,
        long logEpoch) {
}
