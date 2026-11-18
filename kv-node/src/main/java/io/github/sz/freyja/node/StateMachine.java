package io.github.sz.freyja.node;

import java.io.IOException;
import java.util.List;

import io.github.sz.freyja.common.kv.CommandResult;
import io.github.sz.freyja.raft.LogEntry;

/**
 * What a node applies committed entries to. The node hands over every committed entry once,
 * in the order of the log, starting after {@link #appliedIndex}.
 */
public interface StateMachine {

    /** The index of the last entry applied. After a restart: of the last one that survived. */
    long appliedIndex();

    /**
     * Applies a batch of committed entries, also entries that carry no command.
     *
     * @return one result per entry, in order; {@code null} for an entry without a command
     */
    List<CommandResult> apply(List<LogEntry> entries) throws IOException;
}
