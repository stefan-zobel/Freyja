package io.github.stefan_zobel.freyja.common.admin;

import java.util.List;
import java.util.Objects;

/** What a node answers to an {@link AdminRequest}. */
public sealed interface AdminResponse {

    /**
     * The request was carried out, or, for a change of the cluster, taken by the leader. A
     * change that was taken is in the log; {@link AdminRequest.Status} shows when it has
     * taken effect.
     */
    record Done(String detail) implements AdminResponse {
        public Done {
            Objects.requireNonNull(detail, "detail");
        }
    }

    /**
     * The request was not carried out.
     *
     * @param reason   why, for a person to read
     * @param leaderId the leader as this node knows it, if the reason is that this node is
     *                 not the leader; -1 otherwise or if it knows none
     */
    record Refused(String reason, int leaderId) implements AdminResponse {
        public Refused {
            Objects.requireNonNull(reason, "reason");
        }
    }

    /**
     * A member as the node that answers sees it.
     *
     * @param matchIndex how far its log is known to agree with the log of the leader; -1 if
     *                   the node that answers is not the leader and does not know
     * @param state      how it is, as the leader sees it: {@link #OK}, {@link #SILENT},
     *                   {@link #RECOVERING}, {@link #SNAPSHOT}, or {@link #UNKNOWN}
     */
    record Member(int nodeId, boolean voter, String peerAddress, String clientAddress, long matchIndex,
                  String state) {
        /** Heard from lately, and neither refilling its log nor receiving a snapshot. */
        public static final String OK = "ok";
        /** Not heard from lately. */
        public static final String SILENT = "silent";
        /** Refilling a log that it lost. */
        public static final String RECOVERING = "recovering";
        /** Receiving the state machine of the leader. */
        public static final String SNAPSHOT = "snapshot";
        /** The node that answers is not the leader. */
        public static final String UNKNOWN = "";

        public Member {
            Objects.requireNonNull(peerAddress, "peerAddress");
            Objects.requireNonNull(clientAddress, "clientAddress");
            Objects.requireNonNull(state, "state");
        }
    }

    /**
     * The state of a node.
     *
     * @param role             FOLLOWER, PRE_CANDIDATE, CANDIDATE, or LEADER
     * @param leaderId         the leader as this node knows it; -1 if it knows none
     * @param snapshotIndex    the index its stored log starts after
     * @param logRecovering    whether it is refilling a log that it lost
     * @param spaceAlarm       whether the alarm for lack of space is set
     * @param members          the configuration as its state machine has applied it
     * @param restarts         how often the node was started again after it stopped itself
     * @param snapshotsInstalled how often it took the state machine of another node since its
     *                         last start
     */
    record Status(int nodeId, long clusterId, String software, String role, long term, int leaderId,
                  long lastLogIndex, long commitIndex, long appliedIndex, long snapshotIndex,
                  boolean logRecovering, int clusterVersion, boolean spaceAlarm, List<Member> members,
                  long restarts, long snapshotsInstalled) implements AdminResponse {
        public Status {
            Objects.requireNonNull(software, "software");
            Objects.requireNonNull(role, "role");
            members = List.copyOf(members);
        }
    }
}
