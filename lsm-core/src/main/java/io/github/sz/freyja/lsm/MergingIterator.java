package io.github.sz.freyja.lsm;

import java.io.IOException;
import java.util.List;

import io.github.sz.freyja.lsm.table.InternalIterator;
import io.github.sz.freyja.lsm.table.InternalKey;

/**
 * Merges several iterators into one that yields all their entries in internal-key order,
 * forward or backward.
 *
 * <p>The sources hold disjoint sets of internal keys, because a sequence number is used for
 * one entry only. The number of sources is small (MemTables plus the tables of a scan), so
 * the next key is found by a linear pass.
 */
final class MergingIterator implements InternalIterator {

    private final List<InternalIterator> sources;
    private final byte[][] keys;       // current key of each source, null if exhausted
    private int current = -1;          // index of the source with the next key, or -1
    private boolean backward;          // the direction the sources were positioned in

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
        backward = false;
        for (int i = 0; i < keys.length; i++) {
            InternalIterator source = sources.get(i);
            source.seekToFirst();
            keys[i] = source.valid() ? source.key() : null;
        }
        choose();
    }

    @Override
    public void seek(byte[] internalKey) throws IOException {
        backward = false;
        for (int i = 0; i < keys.length; i++) {
            InternalIterator source = sources.get(i);
            source.seek(internalKey);
            keys[i] = source.valid() ? source.key() : null;
        }
        choose();
    }

    @Override
    public void seekToLast() throws IOException {
        backward = true;
        for (int i = 0; i < keys.length; i++) {
            InternalIterator source = sources.get(i);
            source.seekToLast();
            keys[i] = source.valid() ? source.key() : null;
        }
        choose();
    }

    @Override
    public void seekBefore(byte[] internalKey) throws IOException {
        backward = true;
        for (int i = 0; i < keys.length; i++) {
            InternalIterator source = sources.get(i);
            source.seekBefore(internalKey);
            keys[i] = source.valid() ? source.key() : null;
        }
        choose();
    }

    @Override
    public void next() throws IOException {
        if (backward) {
            throw new IllegalStateException("the iterator was positioned to move backward");
        }
        InternalIterator source = sources.get(current);
        source.next();
        keys[current] = source.valid() ? source.key() : null;
        choose();
    }

    @Override
    public void prev() throws IOException {
        if (!backward) {
            throw new IllegalStateException("the iterator was positioned to move forward");
        }
        InternalIterator source = sources.get(current);
        source.prev();
        keys[current] = source.valid() ? source.key() : null;
        choose();
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

    /** Finds the source with the smallest key, or with the largest when moving backward. */
    private void choose() {
        int chosen = -1;
        for (int i = 0; i < keys.length; i++) {
            if (keys[i] == null) {
                continue;
            }
            if (chosen < 0) {
                chosen = i;
            } else {
                int order = InternalKey.compare(keys[i], keys[chosen]);
                if (backward ? order > 0 : order < 0) {
                    chosen = i;
                }
            }
        }
        current = chosen;
    }
}
