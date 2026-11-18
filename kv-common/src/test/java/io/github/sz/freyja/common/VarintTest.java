package io.github.sz.freyja.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Exhaustive checks of the varint codec (docs/spec/verification.md, section 2.8).
 *
 * <p>The reference implementations below use unsigned division instead of shifts and masks,
 * so they share no logic with the code under test.
 */
class VarintTest {

    // ---- independent references ------------------------------------------------------------

    private static byte[] referenceEncode(long value) {
        byte[] out = new byte[Varint.MAX_LENGTH];
        int n = 0;
        while (Long.compareUnsigned(value, 128) >= 0) {
            out[n++] = (byte) (Long.remainderUnsigned(value, 128) + 128);
            value = Long.divideUnsigned(value, 128);
        }
        out[n++] = (byte) value;
        return Arrays.copyOf(out, n);
    }

    /** Smallest n such that value is below 128^n. */
    private static int referenceLength(long value) {
        int n = 1;
        long rest = Long.divideUnsigned(value, 128);
        while (rest != 0) {
            n++;
            rest = Long.divideUnsigned(rest, 128);
        }
        return n;
    }

    /**
     * Classifies a byte string of at most 9 bytes: the varint ends at the first byte below
     * 0x80; a multi-byte varint must not end in 0x00. Overflow needs 10 bytes and cannot
     * occur here.
     */
    private static int referenceDecodedLength(byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            int b = Byte.toUnsignedInt(bytes[i]);
            if (b < 128) {
                return (b == 0 && i > 0) ? Varint.OVERLONG : i + 1;
            }
        }
        return Varint.TRUNCATED;
    }

    private static long referenceValue(byte[] bytes, int length) {
        long value = 0;
        long factor = 1;
        for (int i = 0; i < length; i++) {
            value += (Byte.toUnsignedInt(bytes[i]) % 128) * factor;
            factor *= 128;
        }
        return value;
    }

    // ---- helpers ---------------------------------------------------------------------------

    private static byte[] encode(long value) {
        byte[] buf = new byte[Varint.encodedLength(value)];
        assertEquals(buf.length, Varint.putUint64(buf, 0, value));
        return buf;
    }

    private static void checkRoundTrip(long value) {
        byte[] expected = referenceEncode(value);
        if (Varint.encodedLength(value) != referenceLength(value)
                || expected.length != referenceLength(value)) {
            fail("wrong length for " + Long.toUnsignedString(value));
        }
        byte[] actual = encode(value);
        if (!Arrays.equals(expected, actual)) {
            fail("wrong encoding for " + Long.toUnsignedString(value) + ": " + hex(actual));
        }
        if (Varint.decodedLength(actual, 0, actual.length) != actual.length) {
            fail("encoding of " + Long.toUnsignedString(value) + " not accepted");
        }
        if (Varint.getUint64(actual, 0, actual.length) != value) {
            fail("round trip failed for " + Long.toUnsignedString(value));
        }
    }

    private static String hex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) values[i];
        }
        return out;
    }

    // ---- round trip and minimal length -----------------------------------------------------

    @Test
    void roundTripForEveryValueBelowTwoToThe21() {
        for (long value = 0; value < (1L << 21); value++) {
            checkRoundTrip(value);
        }
    }

    @Test
    void roundTripAtEveryLengthBoundary() {
        List<Long> values = new ArrayList<>();
        for (int k = 1; k <= 9; k++) {
            long power = 1L << (7 * k);
            values.add(power - 1);
            values.add(power);
            values.add(power + 1);
        }
        values.add(Long.MAX_VALUE);       // 2^63 - 1
        values.add(Long.MIN_VALUE);       // 2^63
        values.add(Long.MIN_VALUE + 1);   // 2^63 + 1
        values.add(-2L);                  // 2^64 - 2
        values.add(-1L);                  // 2^64 - 1, the maximum
        for (long value : values) {
            checkRoundTrip(value);
        }
    }

    @Test
    void lengthChangesExactlyAtPowersOf128() {
        for (int k = 1; k <= 9; k++) {
            long power = 1L << (7 * k);
            assertEquals(k, Varint.encodedLength(power - 1));
            assertEquals(k + 1, Varint.encodedLength(power));
        }
        assertEquals(1, Varint.encodedLength(0));
        assertEquals(Varint.MAX_LENGTH, Varint.encodedLength(-1L));
    }

    // ---- decoder accepts only canonical encodings ------------------------------------------

    @Test
    void everyByteStringOfUpToThreeBytesIsRejectedOrCanonical() {
        long accepted = 0;
        long truncated = 0;
        long overlong = 0;
        for (int length = 0; length <= 3; length++) {
            byte[] input = new byte[length];
            long count = 1L << (8 * length);
            for (long n = 0; n < count; n++) {
                for (int i = 0; i < length; i++) {
                    input[i] = (byte) (n >>> (8 * i));
                }
                int expected = referenceDecodedLength(input);
                int actual = Varint.decodedLength(input, 0, length);
                if (actual != expected) {
                    fail("decodedLength(" + hex(input) + ") = " + actual + ", expected " + expected);
                }
                if (actual > 0) {
                    accepted++;
                    long value = Varint.getUint64(input, 0, length);
                    if (value != referenceValue(input, actual)) {
                        fail("wrong value for " + hex(input));
                    }
                    byte[] canonical = encode(value);
                    if (canonical.length != actual
                            || !Arrays.equals(canonical, 0, actual, input, 0, actual)) {
                        fail("accepted non-canonical encoding " + hex(input));
                    }
                } else if (actual == Varint.TRUNCATED) {
                    truncated++;
                } else {
                    overlong++;
                }
            }
        }
        // Guards against a test that silently checks nothing.
        // Accepted: 128 one-byte strings, 128 * 256 two-byte strings that start with a
        // complete one-byte varint, and so on; the exact numbers are fixed by the format.
        assertEquals(128 + (128 * 256 + 128 * 127)
                + (128 * 65536 + 128 * 127 * 256 + 128 * 128 * 127), accepted);
        assertEquals(1 + 128 + 128 * 128 + 128 * 128 * 128, truncated);
        assertEquals(128 + (128 * 256 + 128 * 128), overlong);
    }

    @Test
    void throwingDecoderAgreesWithCheckForEveryStringOfUpToTwoBytes() {
        for (int length = 0; length <= 2; length++) {
            byte[] input = new byte[length];
            int count = 1 << (8 * length);
            for (int n = 0; n < count; n++) {
                for (int i = 0; i < length; i++) {
                    input[i] = (byte) (n >>> (8 * i));
                }
                boolean valid = Varint.decodedLength(input, 0, length) > 0;
                boolean threw = false;
                try {
                    Varint.getUint64(input, 0, length);
                } catch (DecodeException e) {
                    threw = true;
                }
                if (valid == threw) {
                    fail("decoders disagree on " + hex(input));
                }
            }
        }
    }

    @Test
    void tenByteEncodingsThatExceed64BitsAreRejected() {
        byte[] max = bytes(0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01);
        assertEquals(10, Varint.decodedLength(max, 0, 10));
        assertEquals(-1L, Varint.getUint64(max, 0, 10));

        byte[] twoToThe63 = bytes(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01);
        assertEquals(10, Varint.decodedLength(twoToThe63, 0, 10));
        assertEquals(Long.MIN_VALUE, Varint.getUint64(twoToThe63, 0, 10));

        // Every possible tenth byte other than 0x01 is invalid.
        for (int last = 0; last <= 0xFF; last++) {
            if (last == 0x01) {
                continue;
            }
            byte[] input = bytes(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, last, 0x00);
            int expected = (last == 0x00) ? Varint.OVERLONG : Varint.OVERFLOW;
            assertEquals(expected, Varint.decodedLength(input, 0, input.length), "last byte " + last);
            assertThrows(DecodeException.class, () -> Varint.getUint64(input, 0, input.length));
        }
    }

    @Test
    void everyProperPrefixOfAnEncodingIsTruncated() {
        long[] values = {128, 300, 1L << 21, 1L << 35, Long.MAX_VALUE, -1L};
        for (long value : values) {
            byte[] encoded = encode(value);
            for (int limit = 0; limit < encoded.length; limit++) {
                int end = limit;
                assertEquals(Varint.TRUNCATED, Varint.decodedLength(encoded, 0, end));
                assertThrows(DecodeException.class, () -> Varint.getUint64(encoded, 0, end));
            }
        }
    }

    // ---- the MemorySegment variant ---------------------------------------------------------

    /** Decodes through the segment variant; returns null if it rejects the input. */
    private static Long decodeSegment(MemorySegment segment, long offset, long limit) {
        try {
            return Varint.getUint64(segment, offset, limit);
        } catch (DecodeException e) {
            return null;
        }
    }

    @Test
    void segmentDecoderAgreesWithTheArrayDecoderForEveryAcceptedStringOfUpToThreeBytes() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment offHeap = arena.allocate(8);
            for (int length = 0; length <= 3; length++) {
                byte[] input = new byte[length];
                MemorySegment onHeap = MemorySegment.ofArray(input);
                long count = 1L << (8 * length);
                for (long n = 0; n < count; n++) {
                    for (int i = 0; i < length; i++) {
                        input[i] = (byte) (n >>> (8 * i));
                    }
                    Long expected = Varint.decodedLength(input, 0, length) > 0
                            ? Varint.getUint64(input, 0, length) : null;
                    if (expected == null && length == 3) {
                        // Rejections are checked for every string of up to two bytes; a
                        // thrown exception per three-byte string would take too long.
                        continue;
                    }
                    if (!java.util.Objects.equals(expected, decodeSegment(onHeap, 0, length))) {
                        fail("heap segment decoder differs for " + hex(input));
                    }
                    // Off-heap, at a nonzero offset.
                    MemorySegment.copy(input, 0, offHeap, java.lang.foreign.ValueLayout.JAVA_BYTE, 2, length);
                    if (!java.util.Objects.equals(expected, decodeSegment(offHeap, 2, 2 + length))) {
                        fail("off-heap segment decoder differs for " + hex(input));
                    }
                }
            }
        }
    }

    @Test
    void segmentDecoderHandlesLongEncodingsAndBounds() {
        long[] values = {0, 127, 128, 1L << 35, Long.MAX_VALUE, Long.MIN_VALUE, -1L};
        for (long value : values) {
            byte[] encoded = encode(value);
            MemorySegment segment = MemorySegment.ofArray(encoded);
            assertEquals(value, Varint.getUint64(segment, 0, encoded.length));
            for (int limit = 0; limit < encoded.length; limit++) {
                long end = limit;
                assertThrows(DecodeException.class, () -> Varint.getUint64(segment, 0, end));
            }
        }
        MemorySegment overflow = MemorySegment.ofArray(
                bytes(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02));
        assertThrows(DecodeException.class, () -> Varint.getUint64(overflow, 0, 10));
        MemorySegment overlong = MemorySegment.ofArray(bytes(0x80, 0x00));
        assertThrows(DecodeException.class, () -> Varint.getUint64(overlong, 0, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> Varint.getUint64(overlong, 0, 3));
        assertThrows(IndexOutOfBoundsException.class, () -> Varint.getUint64(overlong, 2, 1));
    }

    // ---- offsets and limits ----------------------------------------------------------------

    @Test
    void decodesInsideALargerBufferAndIgnoresWhatFollows() {
        long[] values = {0, 127, 128, 16383, 16384, 1L << 42, -1L};
        for (long value : values) {
            byte[] buf = new byte[3 + Varint.MAX_LENGTH + 2];
            Arrays.fill(buf, (byte) 0xFF);
            int end = Varint.putUint64(buf, 3, value);
            assertEquals(3 + Varint.encodedLength(value), end);
            assertEquals(end - 3, Varint.decodedLength(buf, 3, buf.length));
            assertEquals(value, Varint.getUint64(buf, 3, buf.length));
            // The bytes before and after the encoding are untouched.
            for (int i = 0; i < 3; i++) {
                assertEquals((byte) 0xFF, buf[i]);
            }
            for (int i = end; i < buf.length; i++) {
                assertEquals((byte) 0xFF, buf[i]);
            }
        }
    }

    @Test
    void putIntoABufferThatIsTooSmallWritesNothing() {
        byte[] buf = new byte[4];
        assertThrows(IndexOutOfBoundsException.class, () -> Varint.putUint64(buf, 2, 1L << 21));
        assertArrayEquals(new byte[4], buf);
        assertThrows(IndexOutOfBoundsException.class, () -> Varint.putUint64(buf, 4, 0));
        assertThrows(IndexOutOfBoundsException.class, () -> Varint.putUint64(buf, -1, 0));
    }

    @Test
    void invalidRangesAreRejected() {
        byte[] buf = new byte[4];
        assertThrows(IndexOutOfBoundsException.class, () -> Varint.getUint64(buf, 0, 5));
        assertThrows(IndexOutOfBoundsException.class, () -> Varint.getUint64(buf, 3, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> Varint.decodedLength(buf, -1, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> Varint.decodedLength(buf, 0, 5));
    }
}
