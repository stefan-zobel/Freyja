package io.github.sz.freyja.raft.sim;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import io.github.sz.freyja.raft.HardState;
import io.github.sz.freyja.raft.LogEntry;
import io.github.sz.freyja.raft.LogView;
import io.github.sz.freyja.raft.Ready;

/**
 * The stable storage of a simulated node: what survives a crash. A write reaches it at the
 * moment the simulation declares the write durable, not before. Entries that a snapshot
 * covers are removed from its start.
 *
 * <p>The store remembers how to take its latest writes back. That is what the loss of the
 * newest files of a log does to the log store of a node (docs/spec/node.md, section 3.3): the
 * log is in a state it once had, and entries that a later write had removed are in it again.
 */
public final class SimLog implements LogView {

    /** The stored entries: the indexes {@code offset + 1} and following. */
    private final List<LogEntry> entries = new ArrayList<>();
    private long offset;
    private HardState hardState = HardState.INITIAL;

    /** How to take one write back: where the log ended before it, and what it removed. */
    private record Undo(long lastBefore, long truncateFrom, List<LogEntry> removed) {
    }

    /** The latest writes that changed the log, oldest first; at most {@value #MAX_UNDO}. */
    private final ArrayDeque<Undo> undo = new ArrayDeque<>();
    private static final int MAX_UNDO = 64;

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
        long lastBefore = lastIndex();
        List<LogEntry> removed = List.of();
        if (truncate != -1) {
            if (truncate <= offset) {
                throw new AssertionError("truncation at " + truncate + " reaches into the snapshot at " + offset);
            }
            if (truncate <= lastIndex()) {
                List<LogEntry> suffix = entries.subList((int) (truncate - offset - 1), entries.size());
                removed = List.copyOf(suffix);
                suffix.clear();
            }
        }
        if (truncate != -1 || !ready.entries().isEmpty()) {
            undo.addLast(new Undo(lastBefore, truncate, removed));
            if (undo.size() > MAX_UNDO) {
                undo.removeFirst();
            }
        }
        for (LogEntry entry : ready.entries()) {
            if (entry.index() != lastIndex() + 1) {
                throw new AssertionError("entry " + entry + " does not follow index " + lastIndex());
            }
            entries.add(entry);
        }
    }

    /** The number of writes that {@link #revert} can take back. */
    int revertible() {
        return undo.size();
    }

    /**
     * Takes the latest {@code writes} writes back: the log is what it was before them, as far
     * as no snapshot covers it. Entries that those writes had removed are in the log again.
     * The hard state stays; it is not stored with the log.
     *
     * @return the number of entries that are in the log again though a write had removed them
     */
    int revert(int writes) {
        int back = 0;
        for (int i = 0; i < writes; i++) {
            Undo last = undo.removeLast();
            // What the write appended goes, and with it what it left of the log it cut.
            long cutFrom = last.truncateFrom() != -1 && last.truncateFrom() <= last.lastBefore()
                    ? last.truncateFrom() : last.lastBefore() + 1;
            cutFrom = Math.max(cutFrom, offset + 1);
            if (cutFrom <= lastIndex()) {
                entries.subList((int) (cutFrom - offset - 1), entries.size()).clear();
            }
            for (LogEntry entry : last.removed()) {
                if (entry.index() == lastIndex() + 1) {
                    entries.add(entry);
                    back++;
                }
            }
        }
        return back;
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
            undo.clear(); // the log was started anew
        }
        offset = index;
    }
}
