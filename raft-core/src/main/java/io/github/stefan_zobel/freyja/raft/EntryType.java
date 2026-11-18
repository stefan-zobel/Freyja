package io.github.stefan_zobel.freyja.raft;

/** What a log entry carries. */
public enum EntryType {
    /** A command of the replicated state machine. */
    COMMAND,
    /** The empty entry a new leader appends in its term. */
    NOOP,
    /** A membership change. */
    CONFIG
}
