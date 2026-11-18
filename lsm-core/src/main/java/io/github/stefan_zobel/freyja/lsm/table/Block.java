package io.github.stefan_zobel.freyja.lsm.table;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;

import io.github.stefan_zobel.freyja.common.DecodeException;
import io.github.stefan_zobel.freyja.common.Varint;

/**
 * Reads the contents of a data or index block written by {@link BlockBuilder}.
 *
 * <p>The contents are a {@link MemorySegment}, so a block can live on the heap or in the
 * off-heap block cache. The segment must stay valid while the block or one of its iterators
 * is in use.
 *
 * <p>The block's checksum has been verified by the caller. The parser still checks every
 * length against the block bounds and throws {@link DecodeException} instead of reading
 * outside them.
 */
public final class Block {

    /** Explicitly little-endian, readable at any alignment. */
    private static final ValueLayout.OfInt UINT32 =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    private final MemorySegment data;
    private final int restartsOffset; // also the end of the entry area
    private final int restartCount;

    /** Wraps block contents held in a byte array. */
    public Block(byte[] contents) {
        this(MemorySegment.ofArray(contents));
    }

    /**
     * @throws DecodeException if the restart array is not consistent with the block size
     */
    public Block(MemorySegment contents) {
        this.data = contents;
        long length = contents.byteSize();
        if (length < 8 || length > Integer.MAX_VALUE) {
            throw new DecodeException("bad block size: " + length + " bytes");
        }
        long count = contents.get(UINT32, length - 4) & 0xFFFF_FFFFL;
        long restartsStart = length - 4 - 4 * count;
        if (count < 1 || restartsStart < 0) {
            throw new DecodeException("bad restart count: " + count);
        }
        this.restartCount = (int) count;
        this.restartsOffset = (int) restartsStart;
        long previous = -1;
        for (int i = 0; i < restartCount; i++) {
            long offset = restartOffset(i);
            boolean inRange = (restartsOffset == 0) ? offset == 0 : offset < restartsOffset;
            if (offset <= previous || !inRange || (i == 0 && offset != 0)) {
                throw new DecodeException("bad restart offset " + offset + " at index " + i);
            }
            previous = offset;
        }
    }

    private long restartOffset(int index) {
        return data.get(UINT32, restartsOffset + 4L * index) & 0xFFFF_FFFFL;
    }

    /** Returns a new iterator, positioned before the first entry. */
    public Iterator iterator(Comparator<byte[]> comparator) {
        return new Iterator(comparator);
    }

    /**
     * A cursor over the entries of the block. All methods throw {@link DecodeException} if
     * the block contents are malformed.
     */
    public final class Iterator {
        private final Comparator<byte[]> comparator;
        private byte[] key = new byte[32];
        private int keyLength;
        private int valueOffset;
        private int valueLength;
        private int currentOffset; // offset of the current entry
        private int nextOffset;   // offset of the entry after the current one
        private int nextRestart;  // index of the first restart point at or after nextOffset
        private boolean valid;

        private Iterator(Comparator<byte[]> comparator) {
            this.comparator = comparator;
        }

        /** Returns whether the iterator is positioned at an entry. */
        public boolean valid() {
            return valid;
        }

        /** Returns a copy of the current key. */
        public byte[] key() {
            requireValid();
            return Arrays.copyOf(key, keyLength);
        }

        /** Returns a copy of the current value. */
        public byte[] value() {
            requireValid();
            return data.asSlice(valueOffset, valueLength).toArray(ValueLayout.JAVA_BYTE);
        }

        /** Moves to the first entry; the iterator is invalid afterward if the block is empty. */
        public void seekToFirst() {
            seekToRestart(0);
            next();
        }

        /** Moves to the first entry whose key is greater than or equal to {@code target}. */
        public void seek(byte[] target) {
            // Find the last restart point whose key is less than the target.
            int left = 0;
            int right = restartCount - 1;
            while (left < right) {
                int mid = (left + right + 1) >>> 1;
                seekToRestart(mid);
                next();
                if (!valid) {
                    throw new DecodeException("restart point without an entry");
                }
                if (compareKeyTo(target) < 0) {
                    left = mid;
                } else {
                    right = mid - 1;
                }
            }
            seekToRestart(left);
            do {
                next();
            } while (valid && compareKeyTo(target) < 0);
        }

        /** Moves to the last entry; the iterator is invalid afterward if the block is empty. */
        public void seekToLast() {
            seekToRestart(restartCount - 1);
            next();
            while (valid && nextOffset < restartsOffset) {
                next();
            }
        }

        /** Moves to the last entry whose key is less than {@code target}. */
        public void seekBefore(byte[] target) {
            seek(target);
            if (valid) {
                prev();
            } else {
                seekToLast();
            }
        }

        /**
         * Moves to the entry before the current one; the iterator is invalid afterward if
         * there is none. Keys are stored as differences to their predecessors, so the way
         * back leads over the restart point before the current entry.
         */
        public void prev() {
            requireValid();
            int target = currentOffset;
            if (target == 0) {
                valid = false;
                return;
            }
            // The last restart point before the current entry. The first one is at offset 0.
            int left = 0;
            int right = restartCount - 1;
            while (left < right) {
                int mid = (left + right + 1) >>> 1;
                if (restartOffset(mid) < target) {
                    left = mid;
                } else {
                    right = mid - 1;
                }
            }
            seekToRestart(left);
            do {
                next();
            } while (valid && nextOffset < target);
            if (!valid || nextOffset != target) {
                throw new DecodeException("no entry ends at " + target);
            }
        }

        /** Moves to the next entry; the iterator is invalid afterward if there is none. */
        public void next() {
            int pos = nextOffset;
            if (pos >= restartsOffset) {
                valid = false;
                return;
            }
            currentOffset = pos;
            boolean atRestart = nextRestart < restartCount && restartOffset(nextRestart) == pos;
            if (atRestart) {
                nextRestart++;
            } else if (nextRestart < restartCount && restartOffset(nextRestart) < pos) {
                throw new DecodeException("restart offset " + restartOffset(nextRestart)
                        + " is not the start of an entry");
            }

            long shared = Varint.getUint64(data, pos, restartsOffset);
            pos += Varint.encodedLength(shared);
            long unshared = Varint.getUint64(data, pos, restartsOffset);
            pos += Varint.encodedLength(unshared);
            long valueLen = Varint.getUint64(data, pos, restartsOffset);
            pos += Varint.encodedLength(valueLen);

            long remaining = restartsOffset - pos;
            if (shared < 0 || unshared < 0 || valueLen < 0
                    || unshared > remaining || valueLen > remaining - unshared) {
                throw new DecodeException("entry at " + nextOffset + " exceeds the block");
            }
            if (shared > keyLength || (atRestart && shared != 0)) {
                throw new DecodeException("bad shared key length " + shared + " at " + nextOffset);
            }
            int newKeyLength = (int) (shared + unshared);
            if (newKeyLength > key.length) {
                key = Arrays.copyOf(key, Math.max(newKeyLength, key.length * 2));
            }
            MemorySegment.copy(data, ValueLayout.JAVA_BYTE, pos, key, (int) shared, (int) unshared);
            keyLength = newKeyLength;
            valueOffset = pos + (int) unshared;
            valueLength = (int) valueLen;
            nextOffset = valueOffset + valueLength;
            valid = true;
        }

        private void seekToRestart(int index) {
            nextOffset = (int) restartOffset(index);
            nextRestart = index;
            keyLength = 0;
            valid = false;
        }

        private int compareKeyTo(byte[] target) {
            return comparator.compare(Arrays.copyOf(key, keyLength), target);
        }

        private void requireValid() {
            if (!valid) {
                throw new IllegalStateException("iterator is not positioned at an entry");
            }
        }
    }
}
