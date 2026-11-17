package io.github.stefan_zobel.freyja.lsm.table;

import java.io.Closeable;
import java.io.IOException;

/**
 * A forward cursor over entries in internal-key order. MemTables, table files, and merges of
 * them all look like this to a scan.
 *
 * <p>A new iterator is not positioned; call {@link #seekToFirst()} or {@link #seek} first.
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

    /** Returns the internal key of the current entry. */
    byte[] key() throws IOException;

    /** Returns the value of the current entry. */
    byte[] value() throws IOException;

    /** Releases what the iterator holds. Closing twice has no effect. */
    @Override
    void close() throws IOException;
}
