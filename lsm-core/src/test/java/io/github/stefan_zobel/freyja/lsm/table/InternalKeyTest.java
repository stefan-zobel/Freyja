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

import org.junit.jupiter.api.Test;

/**
 * Exhaustive check of the internal-key order (docs/spec/verification.md, section 2.8).
 */
class InternalKeyTest {

    private static final int[] ALPHABET = {0x00, 0x01, 0x7F, 0x80, 0xFF};
    private static final long[] SEQUENCES =
            {0, 1, 255, 256, InternalKey.MAX_SEQUENCE - 1, InternalKey.MAX_SEQUENCE};
    private static final byte[] TYPES = {InternalKey.TYPE_TOMBSTONE, InternalKey.TYPE_VALUE};

    /** The parts of an internal key, kept apart so the reference never looks at encoded bytes. */
    private record Parts(byte[] userKey, long sequence, byte type) {
    }

    private static List<byte[]> userKeys() {
        List<byte[]> keys = new ArrayList<>();
        keys.add(new byte[0]);
        for (int a : ALPHABET) {
            keys.add(new byte[] {(byte) a});
            for (int b : ALPHABET) {
                keys.add(new byte[] {(byte) a, (byte) b});
                for (int c : ALPHABET) {
                    keys.add(new byte[] {(byte) a, (byte) b, (byte) c});
                }
            }
        }
        return keys;
    }

    private static List<Parts> allParts() {
        List<Parts> all = new ArrayList<>();
        for (byte[] userKey : userKeys()) {
            for (long sequence : SEQUENCES) {
                for (byte type : TYPES) {
                    all.add(new Parts(userKey, sequence, type));
                }
            }
        }
        return all;
    }

    /** Reference order: user key ascending, sequence descending, type ascending. */
    private static int reference(Parts a, Parts b) {
        byte[] x = a.userKey();
        byte[] y = b.userKey();
        for (int i = 0; i < x.length && i < y.length; i++) {
            int p = x[i] < 0 ? x[i] + 256 : x[i];
            int q = y[i] < 0 ? y[i] + 256 : y[i];
            if (p != q) {
                return p < q ? -1 : 1;
            }
        }
        if (x.length != y.length) {
            return x.length < y.length ? -1 : 1;
        }
        if (a.sequence() != b.sequence()) {
            return a.sequence() > b.sequence() ? -1 : 1;
        }
        return Integer.compare(a.type(), b.type());
    }

    private static String describe(Parts p) {
        return "(" + HexFormat.of().formatHex(p.userKey()) + ", " + p.sequence() + ", " + p.type() + ")";
    }

    @Test
    void orderEqualsTheReferenceForEveryPair() {
        List<Parts> parts = allParts();
        assertEquals(156 * 6 * 2, parts.size());
        byte[][] encoded = new byte[parts.size()][];
        for (int i = 0; i < encoded.length; i++) {
            Parts p = parts.get(i);
            encoded[i] = InternalKey.encode(p.userKey(), p.sequence(), p.type());
        }

        long plainOrderDiffers = 0;
        for (int i = 0; i < encoded.length; i++) {
            for (int j = 0; j < encoded.length; j++) {
                int expected = reference(parts.get(i), parts.get(j));
                int actual = Integer.signum(InternalKey.compare(encoded[i], encoded[j]));
                if (actual != expected) {
                    fail("compare" + describe(parts.get(i)) + describe(parts.get(j)) + " = " + actual);
                }
                if (Integer.signum(Arrays.compareUnsigned(encoded[i], encoded[j])) != expected) {
                    plainOrderDiffers++;
                }
            }
        }
        // Comparing the concatenated bytes is wrong for some pairs, so this test cannot be
        // passed by an implementation that does so.
        assertTrue(plainOrderDiffers > 0);
    }

    @Test
    void plainBytewiseComparisonIsWrongForAKnownPair() {
        // User key [01] sorts before [01 00], but the trailer of the first starts with 0xFF.
        byte[] shortKey = InternalKey.encode(new byte[] {0x01}, 0, InternalKey.TYPE_VALUE);
        byte[] longKey = InternalKey.encode(new byte[] {0x01, 0x00}, 0, InternalKey.TYPE_VALUE);
        assertTrue(InternalKey.compare(shortKey, longKey) < 0);
        assertTrue(Arrays.compareUnsigned(shortKey, longKey) > 0);
    }

    @Test
    void encodingRoundTripsForEveryCombination() {
        for (Parts p : allParts()) {
            byte[] key = InternalKey.encode(p.userKey(), p.sequence(), p.type());
            assertEquals(p.userKey().length + InternalKey.TRAILER_LENGTH, key.length);
            assertArrayEquals(p.userKey(), InternalKey.userKey(key));
            assertEquals(p.sequence(), InternalKey.sequence(key));
            assertEquals(p.type(), InternalKey.type(key));
            assertTrue(InternalKey.isWellFormed(key));
            assertTrue(InternalKey.hasUserKey(key, p.userKey()));
        }
    }

    @Test
    void trailerLayoutIsComplementedBigEndianSequenceThenType() {
        byte[] key = InternalKey.encode(new byte[] {0x41}, 0x0102030405060708L, InternalKey.TYPE_VALUE);
        assertArrayEquals(new byte[] {
            0x41,
            (byte) 0xFE, (byte) 0xFD, (byte) 0xFC, (byte) 0xFB,
            (byte) 0xFA, (byte) 0xF9, (byte) 0xF8, (byte) 0xF7,
            0x01}, key);
    }

    @Test
    void seekKeySeparatesVisibleFromInvisibleVersions() {
        byte[] userKey = {0x10, 0x20};
        for (long snapshot : SEQUENCES) {
            byte[] seek = InternalKey.seekKey(userKey, snapshot);
            for (long sequence : SEQUENCES) {
                for (byte type : TYPES) {
                    byte[] entry = InternalKey.encode(userKey, sequence, type);
                    boolean visible = sequence <= snapshot;
                    assertEquals(visible, InternalKey.compare(seek, entry) <= 0,
                            "snapshot " + snapshot + ", entry sequence " + sequence);
                }
            }
        }
    }

    @Test
    void malformedKeysAreRecognized() {
        assertFalse(InternalKey.isWellFormed(new byte[8]));
        byte[] badType = InternalKey.encode(new byte[] {1}, 7, InternalKey.TYPE_VALUE);
        badType[badType.length - 1] = 0x02;
        assertFalse(InternalKey.isWellFormed(badType));
        // A complemented sequence with the top bit clear is a negative sequence number.
        byte[] negative = new byte[10];
        assertFalse(InternalKey.isWellFormed(negative));

        assertThrows(IllegalArgumentException.class, () -> InternalKey.encode(new byte[1], -1, InternalKey.TYPE_VALUE));
        assertThrows(IllegalArgumentException.class, () -> InternalKey.encode(new byte[1], 1, (byte) 2));
        assertThrows(IllegalArgumentException.class, () -> InternalKey.compare(new byte[8], new byte[9]));
    }
}
