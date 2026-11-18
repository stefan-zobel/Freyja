package io.github.stefan_zobel.freyja.common.kv;

import java.util.List;
import java.util.Objects;

/** What a node answers to a {@link ClientRequest}. */
public sealed interface ClientResponse {

    /**
     * The command was applied.
     *
     * @param index the log index at which it was applied
     */
    record Result(CommandResult result, long index) implements ClientResponse {
        public Result {
            Objects.requireNonNull(result, "result");
        }
    }

    /**
     * The answer to a {@link ClientRequest.Get}.
     *
     * @param found        whether the key has a value
     * @param value        the value; empty if not found
     * @param modIndex     the log index of the command that wrote the value; 0 if not found
     * @param appliedIndex the applied index of the node at the moment of the read
     */
    record Value(boolean found, byte[] value, long modIndex, long appliedIndex) implements ClientResponse {
        public Value {
            Objects.requireNonNull(value, "value");
        }
    }

    /** One pair of a scan. */
    record Pair(byte[] key, byte[] value, long modIndex) {
        public Pair {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
        }
    }

    /**
     * The answer to a {@link ClientRequest.Scan}.
     *
     * @param appliedIndex the applied index of the node at the moment of the read
     */
    record Pairs(List<Pair> pairs, long appliedIndex) implements ClientResponse {
        public Pairs {
            pairs = List.copyOf(pairs);
        }
    }

    /**
     * The node is not the leader, or lost its leadership before it could answer. A command
     * that was sent may still be applied; the client sends it again, with the same sequence
     * number, to the leader.
     *
     * @param leaderId      the node this one believes to be the leader; -1 if it knows none
     * @param leaderAddress the address of that node for clients; empty if not known
     */
    record NotLeader(int leaderId, String leaderAddress) implements ClientResponse {
        public NotLeader {
            Objects.requireNonNull(leaderAddress, "leaderAddress");
        }
    }

    /** The request was not carried out. */
    record Rejected(Reason reason) implements ClientResponse {
        public Rejected {
            Objects.requireNonNull(reason, "reason");
        }
    }

    /** Why a request was not carried out. */
    enum Reason {
        /** The node has too much to do. Ask again later, with backoff. */
        OVERLOADED,
        /** The time the request allowed has passed. */
        EXPIRED,
        /** The node cannot serve now: it is starting, stopping, or repairing itself. */
        UNAVAILABLE,
        /** The request is not valid, or asks for a command that clients may not send. */
        INVALID,
        /** The node does not offer what was asked for. */
        UNSUPPORTED
    }
}
