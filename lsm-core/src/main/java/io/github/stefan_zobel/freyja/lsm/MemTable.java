package io.github.stefan_zobel.freyja.lsm;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import io.github.stefan_zobel.freyja.lsm.table.InternalIterator;
import io.github.stefan_zobel.freyja.lsm.table.InternalKey;
import io.github.stefan_zobel.freyja.lsm.table.Lookup;

/**
 * The in-memory part of the tree: a concurrent skip list ordered by internal key, with
 * approximate byte accounting (docs/spec/storage-engine.md, section 4).
 *
 * <p>One writer at a time adds entries; any number of readers may read concurrently. Entries
 * are never changed or removed.
 */
public final class MemTable {

    /** Estimated heap overhead of one entry beyond its key and value bytes. */
    private static final int ENTRY_OVERHEAD = 64;

    private final ConcurrentSkipListMap<byte[], byte[]> entries =
            new ConcurrentSkipListMap<>(InternalKey.COMPARATOR);
    private final AtomicLong approximateBytes = new AtomicLong();

    /** Adds a value for {@code userKey} at {@code sequence}. */
    public void put(long sequence, byte[] userKey, byte[] value) {
        add(InternalKey.encode(userKey, sequence, InternalKey.TYPE_VALUE), value);
    }

    /** Adds a deletion marker for {@code userKey} at {@code sequence}. */
    public void delete(long sequence, byte[] userKey) {
        add(InternalKey.encode(userKey, sequence, InternalKey.TYPE_TOMBSTONE), new byte[0]);
    }

    private void add(byte[] internalKey, byte[] value) {
        if (entries.putIfAbsent(internalKey, value) != null) {
            throw new IllegalStateException("sequence number used twice for one key");
        }
        approximateBytes.addAndGet(internalKey.length + value.length + ENTRY_OVERHEAD);
    }

    /**
     * Looks up the newest entry of {@code userKey} whose sequence number is at most
     * {@code snapshotSequence}.
     */
    public Lookup get(byte[] userKey, long snapshotSequence) {
        Map.Entry<byte[], byte[]> entry =
                entries.ceilingEntry(InternalKey.seekKey(userKey, snapshotSequence));
        if (entry == null || !InternalKey.hasUserKey(entry.getKey(), userKey)) {
            return Lookup.NOT_FOUND;
        }
        return Lookup.of(entry.getKey(), entry.getValue());
    }

    /** Returns all entries in internal-key order. The view reflects later additions. */
    public Iterable<Map.Entry<byte[], byte[]>> entries() {
        return entries.entrySet();
    }

    /**
     * Returns a cursor over the entries. It sees entries added after its creation if they
     * lie ahead of its position, in the direction it moves.
     */
    public InternalIterator iterator() {
        return new InternalIterator() {
            private Iterator<Map.Entry<byte[], byte[]>> source;
            private Map.Entry<byte[], byte[]> current;

            @Override
            public boolean valid() {
                return current != null;
            }

            @Override
            public void seekToFirst() {
                source = entries.entrySet().iterator();
                next();
            }

            @Override
            public void seek(byte[] internalKey) {
                source = entries.tailMap(internalKey, true).entrySet().iterator();
                next();
            }

            @Override
            public void seekToLast() {
                source = entries.descendingMap().entrySet().iterator();
                next();
            }

            @Override
            public void seekBefore(byte[] internalKey) {
                source = entries.headMap(internalKey, false).descendingMap().entrySet().iterator();
                next();
            }

            @Override
            public void next() {
                current = source.hasNext() ? source.next() : null;
            }

            @Override
            public void prev() {
                // The source runs in the direction the cursor was positioned in.
                next();
            }

            @Override
            public byte[] key() {
                return current.getKey();
            }

            @Override
            public byte[] value() {
                return current.getValue();
            }

            @Override
            public void close() {
                current = null;
            }
        };
    }

    /** Returns whether the table holds no entry. */
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    /** Returns the approximate heap size of the table in bytes. */
    public long approximateBytes() {
        return approximateBytes.get();
    }
}
