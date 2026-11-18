package io.github.stefan_zobel.freyja.lsm;

import java.io.Closeable;
import java.io.IOException;

import io.github.stefan_zobel.freyja.common.ByteComparator;
import io.github.stefan_zobel.freyja.lsm.table.InternalIterator;
import io.github.stefan_zobel.freyja.lsm.table.InternalKey;

/**
 * A forward scan over the keys of a range as they were at one sequence number.
 *
 * <p>For every user key the scan returns the newest value whose sequence number is at most
 * the snapshot's; keys whose newest such entry is a deletion are skipped. Keys come in
 * unsigned byte order.
 *
 * <p>The iterator holds table files and cached blocks. It must be closed, and it must be
 * closed before the engine is. Not safe for concurrent use.
 */
public final class ScanIterator implements Closeable {

    private final InternalIterator merged;
    private final long snapshot;
    private final byte[] end; // exclusive; null means no upper bound
    private final Closeable onClose;

    private byte[] key;
    private byte[] value;
    private boolean closed;

    /**
     * @param merged   all entries that may be in the range, in internal-key order
     * @param snapshot the sequence number to read at
     * @param start    first user key of the range, or {@code null} for the first key at all
     * @param end      user key at which the range ends (exclusive), or {@code null}
     * @param onClose  closed once when the iterator is closed
     */
    ScanIterator(InternalIterator merged, long snapshot, byte[] start, byte[] end,
                 Closeable onClose) throws IOException {
        this.merged = merged;
        this.snapshot = snapshot;
        this.end = end;
        this.onClose = onClose;
        try {
            if (start == null) {
                merged.seekToFirst();
            } else {
                // The smallest internal key of the user key: the largest sequence number.
                merged.seek(InternalKey.seekKey(start, InternalKey.MAX_SEQUENCE));
            }
            advance(null);
        } catch (IOException | RuntimeException e) {
            try {
                close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    /** Returns whether the iterator is positioned at an entry. */
    public boolean valid() {
        return key != null;
    }

    /** Returns the current user key. */
    public byte[] key() {
        requireValid();
        return key;
    }

    /** Returns the current value. */
    public byte[] value() {
        requireValid();
        return value;
    }

    /**
     * Moves to the next key of the range.
     *
     * @throws CorruptionException if a block needed for the scan is damaged
     */
    public void next() throws IOException {
        requireValid();
        byte[] previous = key;
        merged.next();
        advance(previous);
    }

    /** Releases the table files and blocks the scan holds. Closing twice has no effect. */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        key = null;
        value = null;
        try {
            merged.close();
        } finally {
            onClose.close();
        }
    }

    /**
     * Moves the merged iterator forward to the next entry the scan returns.
     *
     * @param done a user key that is finished, so that its remaining versions are skipped
     */
    private void advance(byte[] done) throws IOException {
        key = null;
        value = null;
        while (merged.valid()) {
            byte[] internalKey = merged.key();
            if ((done != null && InternalKey.hasUserKey(internalKey, done))
                    || InternalKey.sequence(internalKey) > snapshot) {
                merged.next(); // an older version of a finished key, or newer than the snapshot
                continue;
            }
            // The newest version of this user key that the snapshot sees.
            byte[] userKey = InternalKey.userKey(internalKey);
            if (end != null && ByteComparator.compare(userKey, end) >= 0) {
                return;
            }
            if (InternalKey.type(internalKey) == InternalKey.TYPE_TOMBSTONE) {
                done = userKey;
                merged.next();
                continue;
            }
            key = userKey;
            value = merged.value();
            return;
        }
    }

    private void requireValid() {
        if (closed) {
            throw new IllegalStateException("scan is closed");
        }
        if (key == null) {
            throw new IllegalStateException("scan is not positioned at an entry");
        }
    }
}
