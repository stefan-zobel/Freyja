package io.github.sz.freyja.lsm.table;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.KeyHash;

/**
 * Checks of the blocked Bloom filter (docs/spec/verification.md, section 2.8).
 */
class BloomFilterTest {

    private static long hashOf(String key) {
        return KeyHash.hash(key.getBytes(StandardCharsets.UTF_8));
    }

    private static BloomFilter filterFor(int keyCount, String prefix) {
        long[] hashes = new long[keyCount];
        for (int i = 0; i < keyCount; i++) {
            hashes[i] = hashOf(prefix + i);
        }
        return new BloomFilter(BloomFilter.build(hashes, keyCount));
    }

    @Test
    void noFalseNegativeForAnyFilterSizeFromOneToOneThousandKeys() {
        for (int keyCount = 1; keyCount <= 1000; keyCount++) {
            String prefix = "size-" + keyCount + "-key-";
            BloomFilter filter = filterFor(keyCount, prefix);
            for (int i = 0; i < keyCount; i++) {
                if (!filter.mayContain(hashOf(prefix + i))) {
                    fail("false negative for key " + i + " of " + keyCount);
                }
            }
        }
    }

    @Test
    void noFalseNegativeForArbitraryHashValues() {
        Random random = new Random(42);
        long[] hashes = new long[5000];
        for (int i = 0; i < hashes.length; i++) {
            hashes[i] = random.nextLong();
        }
        // Extreme values exercise the bucket and bit arithmetic at its limits.
        hashes[0] = 0;
        hashes[1] = -1L;
        hashes[2] = Long.MIN_VALUE;
        hashes[3] = Long.MAX_VALUE;
        hashes[4] = 0xFFFFFFFF00000000L;
        hashes[5] = 0x00000000FFFFFFFFL;
        for (int count : new int[] {6, 51, 52, 1000, 5000}) {
            BloomFilter filter = new BloomFilter(BloomFilter.build(hashes, count));
            for (int i = 0; i < count; i++) {
                assertTrue(filter.mayContain(hashes[i]), "hash index " + i + " of " + count);
            }
        }
    }

    @Test
    void falsePositiveRateIsLowAtTenBitsPerKey() {
        int keyCount = 20_000;
        BloomFilter filter = filterFor(keyCount, "present-");
        int falsePositives = 0;
        int probes = 200_000;
        for (int i = 0; i < probes; i++) {
            if (filter.mayContain(hashOf("absent-" + i))) {
                falsePositives++;
            }
        }
        double rate = (double) falsePositives / probes;
        // A blocked filter with 10 bits per key and 6 probes is expected near 1.2%.
        assertTrue(rate < 0.02, "false positive rate " + rate);
        assertTrue(rate > 0.002, "suspiciously low false positive rate " + rate);
    }

    @Test
    void bucketCountIsTenBitsPerKeyRoundedUpToWholeBuckets() {
        assertEquals(1, BloomFilter.bucketCountFor(0));
        assertEquals(1, BloomFilter.bucketCountFor(1));
        assertEquals(1, BloomFilter.bucketCountFor(51));   // 510 bits
        assertEquals(2, BloomFilter.bucketCountFor(52));   // 520 bits
        assertEquals(2, BloomFilter.bucketCountFor(102));  // 1020 bits
        assertEquals(3, BloomFilter.bucketCountFor(103));  // 1030 bits
        assertEquals(1954, BloomFilter.bucketCountFor(100_000));
    }

    /**
     * Recomputes the filter bytes from the rules in the format specification, using
     * arbitrary-precision arithmetic, and compares them with the built filter.
     */
    @Test
    void layoutMatchesTheFormatSpecification() {
        Random random = new Random(7);
        for (int keyCount : new int[] {1, 3, 51, 52, 200, 777}) {
            long[] hashes = new long[keyCount];
            for (int i = 0; i < keyCount; i++) {
                hashes[i] = random.nextLong();
            }
            int buckets = Math.max(1, (keyCount * 10 + 511) / 512);
            byte[] expected = new byte[buckets * 64 + 5];
            BigInteger two32 = BigInteger.ONE.shiftLeft(32);
            for (long hash : hashes) {
                BigInteger unsigned = new BigInteger(Long.toUnsignedString(hash));
                BigInteger high = unsigned.shiftRight(32);
                BigInteger low = unsigned.mod(two32);
                int bucket = high.multiply(BigInteger.valueOf(buckets)).shiftRight(32).intValueExact();
                // delta = low rotated left by 15 within 32 bits
                BigInteger delta = low.shiftLeft(15).mod(two32).or(low.shiftRight(17));
                BigInteger x = low;
                for (int probe = 0; probe < 6; probe++) {
                    int bit = x.mod(BigInteger.valueOf(512)).intValueExact();
                    expected[bucket * 64 + bit / 8] |= (byte) (1 << (bit % 8));
                    x = x.add(delta).mod(two32);
                }
            }
            expected[buckets * 64] = (byte) buckets;
            expected[buckets * 64 + 1] = (byte) (buckets >>> 8);
            expected[buckets * 64 + 4] = 6;
            assertArrayEquals(expected, BloomFilter.build(hashes, keyCount), "key count " + keyCount);

            BloomFilter filter = new BloomFilter(expected);
            assertEquals(buckets, filter.bucketCount());
            assertEquals(6, filter.probeCount());
        }
    }

    @Test
    void inconsistentFilterBlocksAreRejected() {
        byte[] valid = BloomFilter.build(new long[] {1, 2, 3}, 3);
        assertEquals(69, valid.length);
        new BloomFilter(valid);

        assertThrows(DecodeException.class, () -> new BloomFilter(new byte[0]));
        assertThrows(DecodeException.class, () -> new BloomFilter(new byte[5]));
        assertThrows(DecodeException.class, () -> new BloomFilter(new byte[68]));

        byte[] wrongCount = valid.clone();
        wrongCount[64] = 2;
        assertThrows(DecodeException.class, () -> new BloomFilter(wrongCount));

        byte[] zeroCount = valid.clone();
        zeroCount[64] = 0;
        assertThrows(DecodeException.class, () -> new BloomFilter(zeroCount));

        byte[] noProbes = valid.clone();
        noProbes[68] = 0;
        assertThrows(DecodeException.class, () -> new BloomFilter(noProbes));
    }

    @Test
    void aFilterWithAnotherProbeCountIsReadAsWritten() {
        // A filter written with fewer probes stays readable: the count is part of the block.
        byte[] contents = new byte[69];
        contents[64] = 1;
        contents[68] = 1;
        long hash = 0x0000000100000005L; // bucket 0, first bit 5
        contents[0] = (byte) (1 << 5);
        BloomFilter filter = new BloomFilter(contents);
        assertTrue(filter.mayContain(hash));
        assertEquals(1, filter.probeCount());
    }
}
