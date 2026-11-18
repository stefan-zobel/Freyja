package io.github.sz.freyja.raft;

import java.util.Objects;

/**
 * One entry of the replicated log.
 *
 * <p>The payload is treated as immutable once it was handed to or returned from the core. The
 * core does not interpret it. Two entries with the same index and term are the same entry, so
 * {@link #equals} of this record (which compares the payload by identity) is not used by the
 * core.
 *
 * @param index   position in the log; the first entry has index 1
 * @param term    term of the leader that created the entry
 * @param type    what the entry carries
 * @param payload the command bytes; empty for {@link EntryType#NOOP}
 */
public record LogEntry(long index, long term, EntryType type, byte[] payload) {

    public LogEntry {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(payload, "payload");
        if (index < 1 || term < 1) {
            throw new IllegalArgumentException("index " + index + ", term " + term);
        }
    }

    /** The size that counts toward message and backlog limits: the payload length. */
    public int size() {
        return payload.length;
    }

    @Override
    public String toString() {
        return "LogEntry[" + index + "@" + term + " " + type + " " + payload.length + " bytes]";
    }
}
