package io.github.sz.freyja.raft;

/**
 * The fixed settings of one node.
 *
 * @param nodeId              the id of this node
 * @param seed                seed of the generator behind the election timeout jitter
 * @param electionTicks       minimum election timeout in ticks; the timeout in force is drawn
 *                            from {@code [electionTicks, 2 * electionTicks)}
 * @param heartbeatTicks      ticks between two heartbeats of a leader
 * @param maxInflightAppends  maximum number of unacknowledged append messages per follower
 * @param maxAppendBytes      maximum size of the entries in one append message
 * @param maxUncommittedBytes size of the uncommitted part of the log of a leader above which
 *                            proposals are rejected
 * @param promotionLag        a learner is promoted only if it lacks at most this many entries
 */
public record RaftConfig(
        int nodeId,
        long seed,
        int electionTicks,
        int heartbeatTicks,
        int maxInflightAppends,
        int maxAppendBytes,
        long maxUncommittedBytes,
        long promotionLag) {

    public RaftConfig {
        if (nodeId < 0) {
            throw new IllegalArgumentException("nodeId " + nodeId);
        }
        if (heartbeatTicks < 1 || electionTicks <= heartbeatTicks) {
            throw new IllegalArgumentException("the election timeout (" + electionTicks
                    + " ticks) must exceed the heartbeat interval (" + heartbeatTicks + " ticks)");
        }
        if (maxInflightAppends < 1 || maxAppendBytes < 1 || maxUncommittedBytes < 1 || promotionLag < 0) {
            throw new IllegalArgumentException("limits must be positive");
        }
    }

    /** The default settings: election after 10 to 19 ticks, a heartbeat on every tick. */
    public static RaftConfig defaults(int nodeId, long seed) {
        return new RaftConfig(nodeId, seed, 10, 1, 256, 1 << 20, 64L << 20, 1000);
    }

    public RaftConfig withTicks(int election, int heartbeat) {
        return new RaftConfig(nodeId, seed, election, heartbeat, maxInflightAppends,
                maxAppendBytes, maxUncommittedBytes, promotionLag);
    }

    public RaftConfig withFlowControl(int inflightAppends, int appendBytes, long uncommittedBytes) {
        return new RaftConfig(nodeId, seed, electionTicks, heartbeatTicks, inflightAppends,
                appendBytes, uncommittedBytes, promotionLag);
    }

    public RaftConfig withNode(int id, long newSeed) {
        return new RaftConfig(id, newSeed, electionTicks, heartbeatTicks, maxInflightAppends,
                maxAppendBytes, maxUncommittedBytes, promotionLag);
    }

    public RaftConfig withPromotionLag(long lag) {
        return new RaftConfig(nodeId, seed, electionTicks, heartbeatTicks, maxInflightAppends,
                maxAppendBytes, maxUncommittedBytes, lag);
    }
}
