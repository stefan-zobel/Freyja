package io.github.stefan_zobel.freyja.common.kv;

/** What applying a {@link Command} gave. */
public sealed interface CommandResult {

    /** The batch was applied. */
    record Written() implements CommandResult {
    }

    /**
     * A conditional batch was applied.
     *
     * @param conditionsHeld whether every condition held, that is, which of the two batches
     *                       was applied
     */
    record ConditionalApplied(boolean conditionsHeld) implements CommandResult {
    }

    /** A session was opened. */
    record SessionRegistered(long sessionId) implements CommandResult {
    }

    /** A keep-alive or a close was applied. */
    record SessionOk() implements CommandResult {
    }

    /** An alarm was set or cleared, or the cluster version is the one asked for. */
    record AdminApplied() implements CommandResult {
    }

    /** The command had no effect. */
    record Failed(Failure failure) implements CommandResult {
    }

    /** Why a command had no effect. */
    enum Failure {
        /** No session with this id is open. It never was, was closed, or has expired. */
        NO_SUCH_SESSION,
        /**
         * The sequence number is below the last one applied in the session: an answer that
         * the state machine no longer has. The client must not send the command again.
         */
        SEQUENCE_TOO_OLD,
        /**
         * The alarm for lack of space is set: only deletes are accepted. The command did not
         * use up its sequence number and may be sent again when the alarm is cleared.
         */
        NO_SPACE,
        /** A key, a value, or the whole command exceeds its limit, or a key is empty. */
        INVALID,
        /** The cluster version asked for is below the one the cluster has. */
        VERSION_TOO_OLD
    }
}
