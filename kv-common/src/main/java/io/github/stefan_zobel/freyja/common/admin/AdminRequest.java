package io.github.stefan_zobel.freyja.common.admin;

import java.util.Objects;

/**
 * What an administrator asks of a node (docs/spec/operations.md, section 1). Changes of the
 * cluster go to the leader; a node that is not the leader names the leader in its refusal.
 */
public sealed interface AdminRequest {

    /** The state of the node that is asked. Every node answers this. */
    record Status() implements AdminRequest {
    }

    /**
     * Announce a node to the cluster and add it as a learner. The node was prepared with an
     * empty configuration and is running.
     *
     * @param peerAddress   where it accepts connections of peers, as {@code host:port}
     * @param clientAddress where it accepts connections of clients, as {@code host:port}
     */
    record AddLearner(int nodeId, String peerAddress, String clientAddress) implements AdminRequest {
        public AddLearner {
            Objects.requireNonNull(peerAddress, "peerAddress");
            Objects.requireNonNull(clientAddress, "clientAddress");
        }
    }

    /**
     * Make a learner that has caught up a voter.
     *
     * @param force do it also if the voters that are well would not be a majority afterward
     */
    record PromoteLearner(int nodeId, boolean force) implements AdminRequest {
    }

    /**
     * Remove a member, voter or learner.
     *
     * @param force do it also if the voters that are well would not be a majority afterward
     */
    record RemoveMember(int nodeId, boolean force) implements AdminRequest {
    }

    /** Hand the leadership over to a voter. */
    record TransferLeadership(int nodeId) implements AdminRequest {
    }

    /** Make the state machine of the node durable and shorten its log now. */
    record Snapshot() implements AdminRequest {
    }

    /** Read and verify every block of the state machine of the node. */
    record Scrub() implements AdminRequest {
    }

    /** Run the compactions that the state machine of the node is due for. */
    record Compact() implements AdminRequest {
    }

    /** Clear the alarm for lack of space. */
    record ClearSpaceAlarm() implements AdminRequest {
    }

    /** Raise the cluster version: the last step of an upgrade. */
    record SetClusterVersion(int version) implements AdminRequest {
    }

    /** Tell the cluster where a node listens now. */
    record SetAddress(int nodeId, String peerAddress, String clientAddress) implements AdminRequest {
        public SetAddress {
            Objects.requireNonNull(peerAddress, "peerAddress");
            Objects.requireNonNull(clientAddress, "clientAddress");
        }
    }
}
