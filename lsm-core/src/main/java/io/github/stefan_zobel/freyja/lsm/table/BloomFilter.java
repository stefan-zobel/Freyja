package io.github.stefan_zobel.freyja.lsm.table;

import io.github.stefan_zobel.freyja.common.DecodeException;
import io.github.stefan_zobel.freyja.common.LittleEndian;

/**
 * The blocked Bloom filter of a table (docs/spec/storage-format.md, section 5).
 *
 * <p>The filter is an array of 512-bit buckets. A key sets and tests all of its bits inside
 * one bucket, so a lookup touches one cache line.
 *
 * <p>For a 64-bit key hash {@code h}:
 * <ul>
 *   <li>bucket index = {@code ((h >>> 32) * numBuckets) >>> 32}</li>
 *   <li>{@code x} = low 32 bits of {@code h}; {@code delta} = {@code x} rotated left by 15</li>
 *   <li>probe {@code i} uses bit {@code x & 511} of the bucket, then {@code x += delta}</li>
 *   <li>bit {@code b} of a bucket is bit {@code b & 7} of its byte {@code b >>> 3}</li>
 * </ul>
 *
 * <p>Layout: bucket bytes + {@code NumBuckets} ({@code uint32}) + {@code NumProbes}
 * ({@code uint8}).
 */
public final class BloomFilter {

    /** Size of one bucket in bytes. */
    public static final int BUCKET_BYTES = 64;

    /** Bits spent per key when sizing a filter. */
    public static final int BITS_PER_KEY = 10;

    /** Number of probes written by this release. */
    public static final int NUM_PROBES = 6;

    private static final int TRAILER_LENGTH = 5;
    private static final int BUCKET_BITS_MASK = BUCKET_BYTES * 8 - 1;

    /** Largest bucket count whose bytes fit in one array together with the trailer. */
    private static final int MAX_BUCKETS = (Integer.MAX_VALUE - 8 - TRAILER_LENGTH) / BUCKET_BYTES;

    private final byte[] data;
    private final int numBuckets;
    private final int numProbes;

    /**
     * Wraps filter block contents.
     *
     * @throws DecodeException if the sizes in the block are inconsistent
     */
    public BloomFilter(byte[] contents) {
        if (contents.length < TRAILER_LENGTH + BUCKET_BYTES) {
            throw new DecodeException("filter block too small: " + contents.length + " bytes");
        }
        long buckets = LittleEndian.getUint32(contents, contents.length - TRAILER_LENGTH);
        int probes = contents[contents.length - 1] & 0xFF;
        if (buckets < 1 || buckets * BUCKET_BYTES != contents.length - TRAILER_LENGTH) {
            throw new DecodeException("filter bucket count " + buckets
                    + " does not match the block size " + contents.length);
        }
        if (probes < 1) {
            throw new DecodeException("filter without probes");
        }
        this.data = contents;
        this.numBuckets = (int) buckets;
        this.numProbes = probes;
    }

    /** Returns the number of buckets a filter for {@code keyCount} keys has. */
    public static int bucketCountFor(long keyCount) {
        long buckets = (keyCount * BITS_PER_KEY + BUCKET_BYTES * 8 - 1) / (BUCKET_BYTES * 8);
        if (buckets > MAX_BUCKETS) {
            throw new IllegalArgumentException("too many keys for one filter: " + keyCount);
        }
        return (int) Math.max(1, buckets);
    }

    /**
     * Builds filter block contents for the first {@code count} key hashes.
     * Duplicate hashes are allowed.
     */
    public static byte[] build(long[] hashes, int count) {
        int buckets = bucketCountFor(count);
        byte[] out = new byte[buckets * BUCKET_BYTES + TRAILER_LENGTH];
        for (int i = 0; i < count; i++) {
            long hash = hashes[i];
            int base = bucketOffset(hash, buckets);
            int x = (int) hash;
            int delta = Integer.rotateLeft(x, 15);
            for (int probe = 0; probe < NUM_PROBES; probe++) {
                int bit = x & BUCKET_BITS_MASK;
                out[base + (bit >>> 3)] |= (byte) (1 << (bit & 7));
                x += delta;
            }
        }
        LittleEndian.putUint32(out, out.length - TRAILER_LENGTH, buckets);
        out[out.length - 1] = (byte) NUM_PROBES;
        return out;
    }

    /**
     * Returns {@code false} only if no key with this hash was added. Returns {@code true}
     * for every added key and, with low probability, for others.
     */
    public boolean mayContain(long hash) {
        int base = bucketOffset(hash, numBuckets);
        int x = (int) hash;
        int delta = Integer.rotateLeft(x, 15);
        for (int probe = 0; probe < numProbes; probe++) {
            int bit = x & BUCKET_BITS_MASK;
            if ((data[base + (bit >>> 3)] & (1 << (bit & 7))) == 0) {
                return false;
            }
            x += delta;
        }
        return true;
    }

    /** Returns the number of buckets. */
    public int bucketCount() {
        return numBuckets;
    }

    /** Returns the number of probes per key. */
    public int probeCount() {
        return numProbes;
    }

    private static int bucketOffset(long hash, int buckets) {
        return (int) (((hash >>> 32) * buckets) >>> 32) * BUCKET_BYTES;
    }
}
