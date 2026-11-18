package io.github.stefan_zobel.freyja.lsm.table;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.common.ByteComparator;
import io.github.stefan_zobel.freyja.common.DecodeException;
import io.github.stefan_zobel.freyja.common.testing.Seeds;

/**
 * Checks of block prefix compression (docs/spec/verification.md, section 2.8) and of the
 * parser's behavior on malformed blocks.
 */
class BlockTest {

    private static final int[] ALPHABET = {0x00, 0x01, 0x7F, 0x80, 0xFF};

    /** All strings of length 0..maxLength over the alphabet, in unsigned byte order. */
    private static byte[][] sortedStrings(int maxLength) {
        List<byte[]> strings = new ArrayList<>();
        strings.add(new byte[0]);
        for (int a : ALPHABET) {
            strings.add(new byte[] {(byte) a});
            if (maxLength < 2) {
                continue;
            }
            for (int b : ALPHABET) {
                strings.add(new byte[] {(byte) a, (byte) b});
                if (maxLength < 3) {
                    continue;
                }
                for (int c : ALPHABET) {
                    strings.add(new byte[] {(byte) a, (byte) b, (byte) c});
                }
            }
        }
        strings.sort(ByteComparator.UNSIGNED);
        return strings.toArray(new byte[0][]);
    }

    /** A value that depends on the position, with lengths 0, 1, and 2. */
    private static byte[] valueFor(int position) {
        byte[] value = new byte[position % 3];
        Arrays.fill(value, (byte) (0xC0 + position));
        return value;
    }

    private static byte[] build(byte[][] keys, int restartInterval) {
        BlockBuilder builder = new BlockBuilder(restartInterval);
        for (int i = 0; i < keys.length; i++) {
            builder.add(keys[i], valueFor(i));
        }
        byte[] contents = builder.finish();
        assertEquals(contents.length, builder.estimatedSize());
        return contents;
    }

    private static String describe(byte[][] keys, int restartInterval) {
        StringBuilder text = new StringBuilder("interval " + restartInterval + ", keys");
        for (byte[] key : keys) {
            text.append(" [").append(HexFormat.of().formatHex(key)).append(']');
        }
        return text.toString();
    }

    /** Builds a block and checks that a full scan returns exactly the input. */
    private static void checkScan(byte[][] keys, int restartInterval) {
        Block.Iterator it = new Block(build(keys, restartInterval)).iterator(ByteComparator.UNSIGNED);
        it.seekToFirst();
        for (int i = 0; i < keys.length; i++) {
            if (!it.valid() || !Arrays.equals(keys[i], it.key()) || !Arrays.equals(valueFor(i), it.value())) {
                fail("scan mismatch at entry " + i + ": " + describe(keys, restartInterval));
            }
            it.next();
        }
        if (it.valid()) {
            fail("scan returned extra entries: " + describe(keys, restartInterval));
        }
        // The same entries from the end to the start.
        it.seekToLast();
        for (int i = keys.length - 1; i >= 0; i--) {
            if (!it.valid() || !Arrays.equals(keys[i], it.key()) || !Arrays.equals(valueFor(i), it.value())) {
                fail("backward scan mismatch at entry " + i + ": " + describe(keys, restartInterval));
            }
            it.prev();
        }
        if (it.valid()) {
            fail("backward scan returned extra entries: " + describe(keys, restartInterval));
        }
        // A step back and a step forward from every entry.
        for (int i = 0; i < keys.length; i++) {
            it.seek(keys[i]);
            it.prev();
            boolean back = (i == 0) ? !it.valid() : it.valid() && Arrays.equals(keys[i - 1], it.key());
            if (!back) {
                fail("step back from entry " + i + " wrong: " + describe(keys, restartInterval));
            }
            if (i > 0) {
                it.next();
                if (!it.valid() || !Arrays.equals(keys[i], it.key()) || !Arrays.equals(valueFor(i), it.value())) {
                    fail("step forward again to entry " + i + " wrong: " + describe(keys, restartInterval));
                }
            }
        }
    }

    /** Builds a block and checks a seek to every target against a linear search. */
    private static void checkSeeks(byte[][] keys, int restartInterval, byte[][] targets) {
        Block.Iterator it = new Block(build(keys, restartInterval)).iterator(ByteComparator.UNSIGNED);
        for (byte[] target : targets) {
            int expected = 0;
            while (expected < keys.length && ByteComparator.compare(keys[expected], target) < 0) {
                expected++;
            }
            it.seek(target);
            boolean ok = (expected == keys.length)
                    ? !it.valid()
                    : it.valid() && Arrays.equals(keys[expected], it.key())
                            && Arrays.equals(valueFor(expected), it.value());
            if (!ok) {
                fail("seek to [" + HexFormat.of().formatHex(target) + "] wrong: "
                        + describe(keys, restartInterval));
            }
            // The last entry before the target is the one before the first at or after it.
            it.seekBefore(target);
            boolean before = (expected == 0)
                    ? !it.valid()
                    : it.valid() && Arrays.equals(keys[expected - 1], it.key())
                            && Arrays.equals(valueFor(expected - 1), it.value());
            if (!before) {
                fail("seek before [" + HexFormat.of().formatHex(target) + "] wrong: "
                        + describe(keys, restartInterval));
            }
        }
    }

    // ---- exhaustive round trips ------------------------------------------------------------

    @Test
    void everySequenceOfUpToThreeKeysRoundTripsWithEveryRestartInterval() {
        byte[][] all = sortedStrings(3);
        assertEquals(156, all.length);
        long sequences = 0;
        for (int interval = 1; interval <= 4; interval++) {
            for (int a = 0; a < all.length; a++) {
                checkScan(new byte[][] {all[a]}, interval);
                sequences++;
                for (int b = a + 1; b < all.length; b++) {
                    checkScan(new byte[][] {all[a], all[b]}, interval);
                    sequences++;
                    for (int c = b + 1; c < all.length; c++) {
                        checkScan(new byte[][] {all[a], all[b], all[c]}, interval);
                        sequences++;
                    }
                }
            }
        }
        assertEquals(4L * (156 + 156 * 155 / 2 + 156 * 155 * 154 / 6), sequences);
    }

    @Test
    void everySequenceOfFourShortKeysRoundTripsWithEveryRestartInterval() {
        byte[][] all = sortedStrings(2);
        assertEquals(31, all.length);
        for (int interval = 1; interval <= 4; interval++) {
            for (int a = 0; a < all.length; a++) {
                for (int b = a + 1; b < all.length; b++) {
                    for (int c = b + 1; c < all.length; c++) {
                        for (int d = c + 1; d < all.length; d++) {
                            byte[][] keys = {all[a], all[b], all[c], all[d]};
                            checkScan(keys, interval);
                            checkSeeks(keys, interval, all);
                        }
                    }
                }
            }
        }
    }

    @Test
    void seekFindsTheFirstKeyAtOrAfterEveryTarget() {
        byte[][] all = sortedStrings(3);
        for (int interval = 1; interval <= 3; interval++) {
            for (int a = 0; a < all.length; a++) {
                checkSeeks(new byte[][] {all[a]}, interval, all);
                for (int b = a + 1; b < all.length; b++) {
                    checkSeeks(new byte[][] {all[a], all[b]}, interval, all);
                }
            }
        }
    }

    @Test
    void aBlockWithEveryKeyOfTheAlphabetRoundTrips() {
        byte[][] all = sortedStrings(3);
        for (int interval : new int[] {1, 2, 3, 7, 16, 155, 156, 157}) {
            checkScan(all, interval);
            checkSeeks(all, interval, all);
        }
    }

    // ---- structure -------------------------------------------------------------------------

    @Test
    void restartPointsStoreFullKeysAndOthersShareThePrefix() {
        BlockBuilder builder = new BlockBuilder(2);
        builder.add(new byte[] {1, 2, 3}, new byte[] {9});
        builder.add(new byte[] {1, 2, 4}, new byte[0]);
        builder.add(new byte[] {1, 2, 5}, new byte[0]);
        assertArrayEquals(new byte[] {
            0, 3, 1, 1, 2, 3, 9,      // restart: shared 0, unshared 3, value length 1
            2, 1, 0, 4,               // shares [1 2]
            0, 3, 0, 1, 2, 5,         // restart: full key again
            0, 0, 0, 0,               // restart offset 0
            11, 0, 0, 0,              // restart offset 11
            2, 0, 0, 0},              // two restart points
                builder.finish());
    }

    @Test
    void anEmptyBlockHasNoEntries() {
        BlockBuilder builder = new BlockBuilder(16);
        assertTrue(builder.isEmpty());
        byte[] contents = builder.finish();
        assertArrayEquals(new byte[] {0, 0, 0, 0, 1, 0, 0, 0}, contents);
        Block.Iterator it = new Block(contents).iterator(ByteComparator.UNSIGNED);
        it.seekToFirst();
        assertFalse(it.valid());
        it.seek(new byte[] {1});
        assertFalse(it.valid());
        assertThrows(IllegalStateException.class, it::key);
    }

    @Test
    void theBuilderCanBeReused() {
        BlockBuilder builder = new BlockBuilder(16);
        builder.add(new byte[] {1}, new byte[] {1});
        byte[] first = builder.finish();
        builder.reset();
        assertTrue(builder.isEmpty());
        builder.add(new byte[] {1}, new byte[] {1});
        assertArrayEquals(first, builder.finish());
    }

    // ---- randomized, larger blocks ---------------------------------------------------------

    @Test
    void randomBlocksRoundTrip() {
        Seeds.forEach(BlockTest.class, "block-round-trip", 200, seed -> {
            Random random = new Random(seed);
            int count = 1 + random.nextInt(400);
            TreeSet<byte[]> keySet = new TreeSet<>(ByteComparator.UNSIGNED);
            for (int i = 0; i < count; i++) {
                byte[] key = new byte[random.nextInt(24)];
                for (int j = 0; j < key.length; j++) {
                    // A small alphabet produces long shared prefixes.
                    key[j] = (byte) (random.nextInt(4) == 0 ? random.nextInt(256) : random.nextInt(3));
                }
                keySet.add(key);
            }
            byte[][] keys = keySet.toArray(new byte[0][]);
            int interval = 1 + random.nextInt(20);
            checkScan(keys, interval);

            byte[][] targets = new byte[100][];
            for (int i = 0; i < targets.length; i++) {
                byte[] base = keys[random.nextInt(keys.length)];
                targets[i] = random.nextBoolean() ? base
                        : Arrays.copyOf(base, Math.max(0, base.length + random.nextInt(3) - 1));
            }
            checkSeeks(keys, interval, targets);
        });
    }

    // ---- malformed input -------------------------------------------------------------------

    /** Reads everything; the only acceptable failure is a DecodeException. */
    private static void exercise(byte[] contents, byte[][] targets) {
        try {
            Block.Iterator it = new Block(contents).iterator(ByteComparator.UNSIGNED);
            for (it.seekToFirst(); it.valid(); it.next()) {
                it.key();
                it.value();
            }
            for (byte[] target : targets) {
                it.seek(target);
                if (it.valid()) {
                    it.key();
                    it.value();
                }
            }
        } catch (DecodeException expected) {
            // Malformed input was recognized.
        }
    }

    @Test
    void everySingleBitFlipIsParsedWithoutReadingOutsideTheBlock() {
        byte[][] keys = sortedStrings(2);
        byte[] contents = build(keys, 3);
        for (int bit = 0; bit < contents.length * 8; bit++) {
            byte[] damaged = contents.clone();
            damaged[bit >>> 3] ^= (byte) (1 << (bit & 7));
            try {
                exercise(damaged, keys);
            } catch (RuntimeException e) {
                fail("bit " + bit + " caused " + e, e);
            }
        }
    }

    @Test
    void randomBytesAreParsedWithoutReadingOutsideTheBlock() {
        Seeds.forEach(BlockTest.class, "block-garbage", 2000, seed -> {
            Random random = new Random(seed);
            byte[] garbage = new byte[random.nextInt(200)];
            random.nextBytes(garbage);
            if (garbage.length >= 4 && random.nextBoolean()) {
                // Make the restart count plausible so the parser gets past the first check.
                garbage[garbage.length - 4] = (byte) (1 + random.nextInt(3));
                garbage[garbage.length - 3] = 0;
                garbage[garbage.length - 2] = 0;
                garbage[garbage.length - 1] = 0;
            }
            exercise(garbage, new byte[][] {{}, {0x00}, {0x7F}, {(byte) 0xFF, (byte) 0xFF}});
        });
    }

    @Test
    void inconsistentRestartArraysAreRejected() {
        assertThrows(DecodeException.class, () -> new Block(new byte[0]));
        assertThrows(DecodeException.class, () -> new Block(new byte[7]));
        // Restart count 0.
        assertThrows(DecodeException.class, () -> new Block(new byte[] {0, 0, 0, 0, 0, 0, 0, 0}));
        // Restart count larger than the block.
        assertThrows(DecodeException.class, () -> new Block(new byte[] {0, 0, 0, 0, 9, 0, 0, 0}));
        // First restart offset is not 0.
        assertThrows(DecodeException.class,
                () -> new Block(new byte[] {0, 1, 0, 5, 1, 0, 0, 0, 1, 0, 0, 0}));
        // Restart offsets not ascending.
        assertThrows(DecodeException.class,
                () -> new Block(new byte[] {0, 1, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0}));
    }
}
