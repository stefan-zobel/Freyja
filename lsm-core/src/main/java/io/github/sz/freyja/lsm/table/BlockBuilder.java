package io.github.sz.freyja.lsm.table;

import java.util.Arrays;

import io.github.sz.freyja.common.LittleEndian;
import io.github.sz.freyja.common.Varint;

/**
 * Builds the contents of a data or index block (docs/spec/storage-format.md, section 5).
 *
 * <p>Entry: {@code SharedKeyLen} (varint) + {@code UnsharedKeyLen} (varint) + {@code ValueLen}
 * (varint) + key suffix + value. Every {@code restartInterval}-th entry is a restart point
 * and stores its full key. The block ends with the restart offsets ({@code uint32} each) and
 * their count ({@code uint32}).
 *
 * <p>The caller adds keys in ascending order; the builder does not check the order.
 */
public final class BlockBuilder {

    private final int restartInterval;
    private byte[] buffer = new byte[256];
    private int size;
    private int[] restarts = new int[16];
    private int restartCount;
    private int entriesSinceRestart;
    private int entryCount;
    private byte[] lastKey = new byte[0];

    public BlockBuilder(int restartInterval) {
        if (restartInterval < 1) {
            throw new IllegalArgumentException("restart interval must be at least 1");
        }
        this.restartInterval = restartInterval;
    }

    /** Appends an entry. */
    public void add(byte[] key, byte[] value) {
        int shared = 0;
        if (entryCount == 0 || entriesSinceRestart == restartInterval) {
            if (restartCount == restarts.length) {
                restarts = Arrays.copyOf(restarts, restartCount * 2);
            }
            restarts[restartCount++] = size;
            entriesSinceRestart = 0;
        } else {
            int mismatch = Arrays.mismatch(lastKey, key);
            shared = (mismatch < 0) ? key.length : Math.min(mismatch, Math.min(lastKey.length, key.length));
        }
        int unshared = key.length - shared;

        ensureCapacity(3 * Varint.MAX_LENGTH + unshared + value.length);
        size = Varint.putUint64(buffer, size, shared);
        size = Varint.putUint64(buffer, size, unshared);
        size = Varint.putUint64(buffer, size, value.length);
        System.arraycopy(key, shared, buffer, size, unshared);
        size += unshared;
        System.arraycopy(value, 0, buffer, size, value.length);
        size += value.length;

        lastKey = key.clone();
        entriesSinceRestart++;
        entryCount++;
    }

    /** Returns whether no entry has been added. */
    public boolean isEmpty() {
        return entryCount == 0;
    }

    /** Returns the number of entries added. */
    public int entryCount() {
        return entryCount;
    }

    /** Returns the size the finished block would have now. */
    public int estimatedSize() {
        return size + 4 * Math.max(restartCount, 1) + 4;
    }

    /** Returns the size the finished block would have after adding this entry. */
    public int sizeWith(byte[] key, byte[] value) {
        boolean restart = entryCount == 0 || entriesSinceRestart == restartInterval;
        int shared = 0;
        if (!restart) {
            int mismatch = Arrays.mismatch(lastKey, key);
            shared = (mismatch < 0) ? key.length : mismatch;
        }
        int unshared = key.length - shared;
        int entry = Varint.encodedLength(shared) + Varint.encodedLength(unshared)
                + Varint.encodedLength(value.length) + unshared + value.length;
        int restarts = restartCount + (restart ? 1 : 0);
        return size + entry + 4 * restarts + 4;
    }

    /** Returns the block contents. The builder must be {@link #reset()} before reuse. */
    public byte[] finish() {
        if (restartCount == 0) {
            restarts[restartCount++] = 0; // an empty block still has one restart point
        }
        byte[] out = Arrays.copyOf(buffer, size + 4 * restartCount + 4);
        int pos = size;
        for (int i = 0; i < restartCount; i++) {
            LittleEndian.putUint32(out, pos, restarts[i]);
            pos += 4;
        }
        LittleEndian.putUint32(out, pos, restartCount);
        return out;
    }

    /** Clears the builder for the next block. */
    public void reset() {
        size = 0;
        restartCount = 0;
        entriesSinceRestart = 0;
        entryCount = 0;
        lastKey = new byte[0];
    }

    private void ensureCapacity(int extra) {
        long needed = (long) size + extra;
        if (needed > Integer.MAX_VALUE - 8) {
            throw new IllegalStateException("block too large");
        }
        if (needed > buffer.length) {
            buffer = Arrays.copyOf(buffer, (int) Math.max(needed, Math.min(Integer.MAX_VALUE - 8, buffer.length * 2L)));
        }
    }
}
