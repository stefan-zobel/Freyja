package io.github.sz.freyja.raft;

import java.util.Objects;

/** A message to send to another node. */
public record Outbound(int toNode, RaftMessage message) {

    public Outbound {
        Objects.requireNonNull(message, "message");
    }
}
