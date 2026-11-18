package io.github.stefan_zobel.freyja.raft.sim;

import java.util.ArrayList;
import java.util.List;

import io.github.stefan_zobel.freyja.raft.HardState;
import io.github.stefan_zobel.freyja.raft.LogEntry;
import io.github.stefan_zobel.freyja.raft.LogView;
import io.github.stefan_zobel.freyja.raft.Ready;

/**
 * The stable storage of a simulated node: what survives a crash. A write reaches it at the
 * moment the simulation declares the write durable, not before.
 */
public final class SimLog implements LogView {

    private final List<LogEntry> entries = new ArrayList<>();
    private HardState hardState = HardState.INITIAL;

    @Override
    public long firstIndex() {
        return 1;
    }

    @Override
    public long lastIndex() {
        return entries.size();
    }

    @Override
    public long termAt(long index) {
        return index == 0 ? 0 : get(index).term();
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
        return entries.get((int) index - 1);
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
            hardState = next;
        }
        applyTo(entries, ready);
    }

    /** Applies the log changes of {@code ready} to {@code log} and checks their shape. */
    static void applyTo(List<LogEntry> log, Ready ready) {
        long truncate = ready.truncateSuffixFrom();
        if (truncate != -1) {
            if (truncate < 1) {
                throw new AssertionError("truncation at " + truncate);
            }
            if (truncate <= log.size()) {
                log.subList((int) truncate - 1, log.size()).clear();
            }
        }
        for (LogEntry entry : ready.entries()) {
            if (entry.index() != log.size() + 1) {
                throw new AssertionError("entry " + entry + " does not follow index " + log.size());
            }
            if (!log.isEmpty() && log.getLast().term() > entry.term()) {
                throw new AssertionError("entry " + entry + " follows a later term");
            }
            log.add(entry);
        }
    }
}
