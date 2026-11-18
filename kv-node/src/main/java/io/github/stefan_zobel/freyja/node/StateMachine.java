package io.github.stefan_zobel.freyja.node;

import java.io.IOException;

import io.github.stefan_zobel.freyja.raft.LogEntry;

/**
 * What a node applies committed entries to. The node hands over every committed entry once,
 * in the order of the log, starting after {@link #appliedIndex}.
 */
public interface StateMachine {

    /** The index of the last entry applied. After a restart: of the last one that survived. */
    long appliedIndex();

    /** Applies one committed entry, also entries that carry no command. */
    void apply(LogEntry entry) throws IOException;
}
