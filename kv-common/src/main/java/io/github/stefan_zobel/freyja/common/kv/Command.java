package io.github.stefan_zobel.freyja.common.kv;

import java.util.List;
import java.util.Objects;

/**
 * A command of the replicated state machine (docs/spec/node.md, section 4.1). Commands travel
 * through the Raft log; every node applies the same commands in the same order.
 *
 * <p>Keys and values are treated as immutable once they are part of a command.
 */
public sealed interface Command {

    /** Limits of section 4.1. */
    int MAX_KEY_SIZE = 64 * 1024;
    int MAX_VALUE_SIZE = 1024 * 1024;
    int MAX_REQUEST_SIZE = 4 * 1024 * 1024;

    /** A change of one key. */
    sealed interface Mutation {
        byte[] key();
    }

    record Put(byte[] key, byte[] value) implements Mutation {
        public Put {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
        }
    }

    record Delete(byte[] key) implements Mutation {
        public Delete {
            Objects.requireNonNull(key, "key");
        }
    }

    /** What a condition demands of a key. */
    enum ConditionKind {
        /** The key has a value. */
        EXISTS,
        /** The key has no value. */
        NOT_EXISTS,
        /** The key has exactly the given value. */
        VALUE_EQUALS,
        /** The key was last written by the command at the given log index. */
        MOD_INDEX_EQUALS
    }

    /**
     * A condition on one key.
     *
     * @param value    for {@link ConditionKind#VALUE_EQUALS}; otherwise empty
     * @param modIndex for {@link ConditionKind#MOD_INDEX_EQUALS}; otherwise 0
     */
    record Condition(byte[] key, ConditionKind kind, byte[] value, long modIndex) {
        public Condition {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(value, "value");
        }

        public static Condition exists(byte[] key) {
            return new Condition(key, ConditionKind.EXISTS, new byte[0], 0);
        }

        public static Condition notExists(byte[] key) {
            return new Condition(key, ConditionKind.NOT_EXISTS, new byte[0], 0);
        }

        public static Condition valueEquals(byte[] key, byte[] value) {
            return new Condition(key, ConditionKind.VALUE_EQUALS, value, 0);
        }

        public static Condition modIndexEquals(byte[] key, long modIndex) {
            return new Condition(key, ConditionKind.MOD_INDEX_EQUALS, new byte[0], modIndex);
        }
    }

    /**
     * An atomic batch of puts and deletes.
     *
     * @param sessionId the session of the client, from {@link RegisterSession}
     * @param sequence  the number of the command in its session; a command that is applied a
     *                  second time is answered from the record of the first
     */
    record Write(long sessionId, long sequence, List<Mutation> mutations) implements Command {
        public Write {
            mutations = List.copyOf(mutations);
        }
    }

    /**
     * A conditional batch: if every condition holds, {@code then} is applied, otherwise
     * {@code otherwise}.
     */
    record Conditional(long sessionId, long sequence, List<Condition> conditions, List<Mutation> then,
                       List<Mutation> otherwise) implements Command {
        public Conditional {
            conditions = List.copyOf(conditions);
            then = List.copyOf(then);
            otherwise = List.copyOf(otherwise);
        }
    }

    /** Opens a session. The id of the session is the log index of this command. */
    record RegisterSession() implements Command {
    }

    /** Tells that the client of a session is alive. */
    record KeepAlive(long sessionId) implements Command {
    }

    /** Ends a session. */
    record CloseSession(long sessionId) implements Command {
    }
}
