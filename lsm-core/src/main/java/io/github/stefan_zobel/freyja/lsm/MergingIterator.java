package io.github.stefan_zobel.freyja.lsm;

import java.io.IOException;
import java.util.List;

import io.github.stefan_zobel.freyja.lsm.table.InternalIterator;
import io.github.stefan_zobel.freyja.lsm.table.InternalKey;

/**
 * Merges several iterators into one that yields all their entries in internal-key order.
 *
 * <p>The sources hold disjoint sets of internal keys, because a sequence number is used for
 * one entry only. The number of sources is small (MemTables plus the tables of a scan), so
 * the smallest current key is found by a linear pass.
 */
final class MergingIterator implements InternalIterator {

    private final List<InternalIterator> sources;
    private final byte[][] keys;       // current key of each source, null if exhausted
    private int current = -1;          // index of the source with the smallest key, or -1

    MergingIterator(List<InternalIterator> sources) {
        this.sources = sources;
        this.keys = new byte[sources.size()][];
    }

    @Override
    public boolean valid() {
        return current >= 0;
    }

    @Override
    public void seekToFirst() throws IOException {
        for (int i = 0; i < keys.length; i++) {
            InternalIterator source = sources.get(i);
            source.seekToFirst();
            keys[i] = source.valid() ? source.key() : null;
        }
        findSmallest();
    }

    @Override
    public void seek(byte[] internalKey) throws IOException {
        for (int i = 0; i < keys.length; i++) {
            InternalIterator source = sources.get(i);
            source.seek(internalKey);
            keys[i] = source.valid() ? source.key() : null;
        }
        findSmallest();
    }

    @Override
    public void next() throws IOException {
        InternalIterator source = sources.get(current);
        source.next();
        keys[current] = source.valid() ? source.key() : null;
        findSmallest();
    }

    @Override
    public byte[] key() {
        return keys[current];
    }

    @Override
    public byte[] value() throws IOException {
        return sources.get(current).value();
    }

    /** Closes every source. The first failure is thrown after all were closed. */
    @Override
    public void close() throws IOException {
        current = -1;
        IOException first = null;
        for (InternalIterator source : sources) {
            try {
                source.close();
            } catch (IOException e) {
                if (first == null) {
                    first = e;
                } else {
                    first.addSuppressed(e);
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }

    private void findSmallest() {
        int smallest = -1;
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] != null && (smallest < 0 || InternalKey.compare(keys[i], keys[smallest]) < 0)) {
                smallest = i;
            }
        }
        current = smallest;
    }
}
