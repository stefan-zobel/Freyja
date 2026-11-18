package io.github.stefan_zobel.freyja.node;

import io.github.stefan_zobel.freyja.common.kv.CommandResult;
import io.github.stefan_zobel.freyja.raft.LogEntry;
import io.github.stefan_zobel.freyja.raft.Notification;

/** Is told what happens inside a node. It is called from the event loop and must not block. */
@FunctionalInterface
public interface NodeListener {

    /** The core reports an answer to a proposal or a request, or a change of role. */
    void notification(Notification notification);

    /**
     * An entry was applied to the state machine.
     *
     * @param result the result of its command; {@code null} if the entry carries none
     */
    default void applied(LogEntry entry, CommandResult result) {
    }
}
