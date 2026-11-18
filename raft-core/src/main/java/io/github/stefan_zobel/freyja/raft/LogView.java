package io.github.stefan_zobel.freyja.raft;

import java.util.List;

/**
 * Read-only access to the durably stored entries. The runtime implements it.
 *
 * <p>The core reads only indexes at or below its last persisted index, that is, entries of a
 * {@link Ready} for which it has received {@link RaftEvent.Persisted}. The view must show those
 * entries from the moment that event is fed. What it shows beyond them does not matter.
 */
public interface LogView {

    /** Index of the first entry still stored: the snapshot index plus one. */
    long firstIndex();

    /** Index of the last stored entry; {@code firstIndex() - 1} if no entry is stored. */
    long lastIndex();

    /**
     * Term of the entry at {@code index}. Valid for {@code firstIndex() - 1} (the term the
     * snapshot ends with; 0 if there is no snapshot) through {@code lastIndex()}.
     */
    long termAt(long index);

    /**
     * The entries from {@code fromInclusive} up to but excluding {@code toExclusive}, in
     * order, cut off before the sum of their sizes exceeds {@code maxBytes}. The first entry
     * is returned even if it alone exceeds the limit.
     */
    List<LogEntry> entries(long fromInclusive, long toExclusive, int maxBytes);
}
