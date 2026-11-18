package io.github.stefan_zobel.freyja.raft;

import java.util.Objects;

/** Everything that can happen to a node. Events are the only input of the core. */
public sealed interface RaftEvent {

    /** One unit of time has passed. All timeouts are counted in ticks. */
    record Tick() implements RaftEvent {
    }

    /** A message from another node arrived. */
    record MessageReceived(int fromNode, RaftMessage message) implements RaftEvent {
        public MessageReceived {
            Objects.requireNonNull(message, "message");
        }
    }

    /**
     * Everything the {@link Ready}s up to and including {@code readySequence} asked to persist
     * is durable.
     */
    record Persisted(long readySequence) implements RaftEvent {
    }

    /** A client asks to append a command to the log. */
    record ClientPropose(long requestId, byte[] command) implements RaftEvent {
        public ClientPropose {
            Objects.requireNonNull(command, "command");
        }
    }
}
