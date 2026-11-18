package io.github.sz.freyja.lsm.table;

import java.io.Closeable;
import java.io.IOException;

/**
 * A cursor over entries in internal-key order. MemTables, table files, and merges of them
 * all look like this to a scan.
 *
 * <p>A new iterator is not positioned. It is positioned to move forward by
 * {@link #seekToFirst()} or {@link #seek} and then moved by {@link #next()}, or positioned to
 * move backward by {@link #seekToLast()} or {@link #seekBefore} and then moved by
 * {@link #prev()}. The direction changes only by positioning anew.
 * Not safe for concurrent use.
 */
public interface InternalIterator extends Closeable {

    /** Returns whether the iterator is positioned at an entry. */
    boolean valid();

    /** Moves to the first entry. */
    void seekToFirst() throws IOException;

    /** Moves to the first entry whose internal key is at or after {@code internalKey}. */
    void seek(byte[] internalKey) throws IOException;

    /** Moves to the next entry. The iterator must be valid. */
    void next() throws IOException;

    /** Moves to the last entry. */
    void seekToLast() throws IOException;

    /** Moves to the last entry whose internal key is before {@code internalKey}. */
    void seekBefore(byte[] internalKey) throws IOException;

    /** Moves to the entry before the current one. The iterator must be valid. */
    void prev() throws IOException;

    /** Returns the internal key of the current entry. */
    byte[] key() throws IOException;

    /** Returns the value of the current entry. */
    byte[] value() throws IOException;

    /** Releases what the iterator holds. Closing twice has no effect. */
    @Override
    void close() throws IOException;
}
