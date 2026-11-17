package io.github.stefan_zobel.freyja.common;

import java.util.Objects;

/**
 * The 64-bit key hash: XXH64 with seed 0.
 *
 * <p>This function is part of the on-disk format (Bloom filters store bits derived from it).
 * Changing it requires a new format version (docs/spec/storage-format.md, section 5).
 */
public final class KeyHash {

    private static final long PRIME_1 = 0x9E3779B185EBCA87L;
    private static final long PRIME_2 = 0xC2B2AE3D27D4EB4FL;
    private static final long PRIME_3 = 0x165667B19E3779F9L;
    private static final long PRIME_4 = 0x85EBCA77C2B2AE63L;
    private static final long PRIME_5 = 0x27D4EB2F165667C5L;

    private KeyHash() {
    }

    /** Returns the hash of the whole array. */
    public static long hash(byte[] buf) {
        return hash(buf, 0, buf.length);
    }

    /** Returns the hash of {@code length} bytes starting at {@code offset}. */
    public static long hash(byte[] buf, int offset, int length) {
        Objects.checkFromIndexSize(offset, length, buf.length);
        int pos = offset;
        int end = offset + length;
        long h;

        if (length >= 32) {
            long v1 = PRIME_1 + PRIME_2;
            long v2 = PRIME_2;
            long v3 = 0;
            long v4 = -PRIME_1;
            int stripesEnd = end - 32;
            do {
                v1 = round(v1, LittleEndian.getUint64(buf, pos));
                v2 = round(v2, LittleEndian.getUint64(buf, pos + 8));
                v3 = round(v3, LittleEndian.getUint64(buf, pos + 16));
                v4 = round(v4, LittleEndian.getUint64(buf, pos + 24));
                pos += 32;
            } while (pos <= stripesEnd);
            h = Long.rotateLeft(v1, 1) + Long.rotateLeft(v2, 7)
                    + Long.rotateLeft(v3, 12) + Long.rotateLeft(v4, 18);
            h = mergeRound(h, v1);
            h = mergeRound(h, v2);
            h = mergeRound(h, v3);
            h = mergeRound(h, v4);
        } else {
            h = PRIME_5;
        }
        h += length;

        while (end - pos >= 8) {
            h ^= round(0, LittleEndian.getUint64(buf, pos));
            h = Long.rotateLeft(h, 27) * PRIME_1 + PRIME_4;
            pos += 8;
        }
        if (end - pos >= 4) {
            h ^= LittleEndian.getUint32(buf, pos) * PRIME_1;
            h = Long.rotateLeft(h, 23) * PRIME_2 + PRIME_3;
            pos += 4;
        }
        while (pos < end) {
            h ^= (buf[pos] & 0xFFL) * PRIME_5;
            h = Long.rotateLeft(h, 11) * PRIME_1;
            pos++;
        }

        h ^= h >>> 33;
        h *= PRIME_2;
        h ^= h >>> 29;
        h *= PRIME_3;
        h ^= h >>> 32;
        return h;
    }

    private static long round(long acc, long input) {
        acc += input * PRIME_2;
        acc = Long.rotateLeft(acc, 31);
        return acc * PRIME_1;
    }

    private static long mergeRound(long acc, long value) {
        acc ^= round(0, value);
        return acc * PRIME_1 + PRIME_4;
    }
}
