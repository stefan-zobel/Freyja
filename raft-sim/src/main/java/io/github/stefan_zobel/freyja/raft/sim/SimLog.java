package io.github.stefan_zobel.freyja.raft.sim;

import java.util.ArrayList;
import java.util.List;

import io.github.stefan_zobel.freyja.raft.HardState;
import io.github.stefan_zobel.freyja.raft.LogEntry;
import io.github.stefan_zobel.freyja.raft.LogView;
import io.github.stefan_zobel.freyja.raft.Ready;

/**
 * The stable storage of a simulated node: what survives a crash. A write reaches it at the
 * moment the simulation declares the write durable, not before. Entries that a snapshot
 * covers are removed from its start.
 */
public final class SimLog implements LogView {

    /** The stored entries: the indexes {@code offset + 1} and following. */
    private final List<LogEntry> entries = new ArrayList<>();
    private long offset;
    private HardState hardState = HardState.INITIAL;

    @Override
    public long firstIndex() {
        return offset + 1;
    }

    @Override
    public long lastIndex() {
        return offset + entries.size();
    }

    @Override
    public long termAt(long index) {
        return get(index).term();
    }

    @Override
    public List<LogEntry> entries(long fromInclusive, long toExclusive, int maxBytes) {
        List<LogEntry> result = new ArrayList<>();
        long bytes = 0;
        for (long i = fromInclusive; i < toExclusive; i++) {
            LogEntry entry = get(i);
            if (!result.isEmpty() && bytes + entry.size() > maxBytes) {
                break;
            }
            bytes += entry.size();
            result.add(entry);
        }
        return result;
    }

    public LogEntry get(long index) {
        if (index <= offset || index > lastIndex()) {
            throw new AssertionError("entry " + index + " is not stored: " + firstIndex() + ".." + lastIndex());
        }
        return entries.get((int) (index - offset - 1));
    }

    public HardState hardState() {
        return hardState;
    }

    /** A copy of the stored entries. */
    public List<LogEntry> entries() {
        return new ArrayList<>(entries);
    }

    /** Stores what {@code ready} asks to persist, in the order the contract prescribes. */
    void store(Ready ready) {
        if (ready.hardState() != null) {
            HardState next = ready.hardState();
            if (next.term() < hardState.term()) {
                throw new AssertionError("the term went back: " + hardState + " -> " + next);
            }
            if (next.term() == hardState.term() && hardState.votedFor() != HardState.NO_VOTE
                    && next.votedFor() != hardState.votedFor()) {
                throw new AssertionError("the vote changed within a term: " + hardState + " -> " + next);
            }
            if (next.logEpoch() < hardState.logEpoch()) {
                throw new AssertionError("the log epoch went back: " + hardState + " -> " + next);
            }
            hardState = next;
        }
        long truncate = ready.truncateSuffixFrom();
        if (truncate != -1) {
            if (truncate <= offset) {
                throw new AssertionError("truncation at " + truncate + " reaches into the snapshot at " + offset);
            }
            if (truncate <= lastIndex()) {
                entries.subList((int) (truncate - offset - 1), entries.size()).clear();
            }
        }
        for (LogEntry entry : ready.entries()) {
            if (entry.index() != lastIndex() + 1) {
                throw new AssertionError("entry " + entry + " does not follow index " + lastIndex());
            }
            entries.add(entry);
        }
    }

    /** Removes the entries up to {@code through}: a snapshot covers them. */
    void truncatePrefix(long through) {
        if (through <= offset) {
            return;
        }
        if (through >= lastIndex()) {
            entries.clear();
        } else {
            entries.subList(0, (int) (through - offset)).clear();
        }
        offset = through;
    }

    /**
     * A snapshot was installed: the log starts after {@code index}.
     *
     * @param keepSuffix whether the entries after {@code index} stay
     */
    void startAfter(long index, boolean keepSuffix) {
        if (index < offset) {
            throw new AssertionError("a snapshot at " + index + " behind the stored log, which starts after " + offset);
        }
        if (keepSuffix) {
            truncatePrefix(index);
        } else {
            entries.clear();
        }
        offset = index;
    }
}
