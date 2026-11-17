package io.github.stefan_zobel.freyja.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

/**
 * Round trips at every boundary value of every width, compared with a hand-written
 * little-endian reference (docs/spec/verification.md, section 2.8).
 */
class LittleEndianTest {

    private static final byte FILL = (byte) 0xA5;
    private static final int OFFSET = 3;

    /** Hand-written reference: byte i holds bits 8i..8i+7 of the value. */
    private static byte[] reference(long value, int width) {
        byte[] buf = new byte[OFFSET + width + 2];
        Arrays.fill(buf, FILL);
        for (int i = 0; i < width; i++) {
            buf[OFFSET + i] = (byte) (value >>> (8 * i));
        }
        return buf;
    }

    private static byte[] filled(int width) {
        byte[] buf = new byte[OFFSET + width + 2];
        Arrays.fill(buf, FILL);
        return buf;
    }

    /** 0, the maximum, and 2^k - 1, 2^k, 2^k + 1 for every bit k of the width. */
    private static long[] boundaryValues(int bits) {
        long mask = (bits == 64) ? -1L : (1L << bits) - 1;
        TreeSet<Long> values = new TreeSet<>();
        values.add(0L);
        values.add(mask);
        for (int k = 0; k < bits; k++) {
            long power = 1L << k;
            values.add((power - 1) & mask);
            values.add(power & mask);
            values.add((power + 1) & mask);
        }
        return values.stream().mapToLong(Long::longValue).toArray();
    }

    @Test
    void uint16AtEveryBoundary() {
        for (long value : boundaryValues(16)) {
            byte[] buf = filled(2);
            LittleEndian.putUint16(buf, OFFSET, (int) value);
            assertArrayEquals(reference(value, 2), buf, "value " + value);
            assertEquals(value, LittleEndian.getUint16(buf, OFFSET));
        }
    }

    @Test
    void uint16ForEveryValue() {
        for (int value = 0; value <= 0xFFFF; value++) {
            byte[] buf = filled(2);
            LittleEndian.putUint16(buf, OFFSET, value);
            assertArrayEquals(reference(value, 2), buf);
            assertEquals(value, LittleEndian.getUint16(buf, OFFSET));
        }
    }

    @Test
    void uint32AtEveryBoundary() {
        for (long value : boundaryValues(32)) {
            byte[] buf = filled(4);
            LittleEndian.putUint32(buf, OFFSET, value);
            assertArrayEquals(reference(value, 4), buf, "value " + value);
            assertEquals(value, LittleEndian.getUint32(buf, OFFSET));

            // The raw 32-bit accessors use the same bytes.
            assertEquals((int) value, LittleEndian.getInt32(buf, OFFSET));
            byte[] raw = filled(4);
            LittleEndian.putInt32(raw, OFFSET, (int) value);
            assertArrayEquals(buf, raw);
        }
    }

    @Test
    void uint64AtEveryBoundary() {
        for (long value : boundaryValues(64)) {
            byte[] buf = filled(8);
            LittleEndian.putUint64(buf, OFFSET, value);
            assertArrayEquals(reference(value, 8), buf, "value " + Long.toUnsignedString(value));
            assertEquals(value, LittleEndian.getUint64(buf, OFFSET));
        }
    }

    @Test
    void knownByteLayout() {
        byte[] buf = new byte[8];
        LittleEndian.putUint64(buf, 0, 0x0807060504030201L);
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}, buf);
        assertEquals(0x0201, LittleEndian.getUint16(buf, 0));
        assertEquals(0x04030201L, LittleEndian.getUint32(buf, 0));
    }

    @Test
    void valuesOutsideTheWidthAreRejected() {
        byte[] buf = new byte[8];
        assertThrows(IllegalArgumentException.class, () -> LittleEndian.putUint16(buf, 0, -1));
        assertThrows(IllegalArgumentException.class, () -> LittleEndian.putUint16(buf, 0, 0x10000));
        assertThrows(IllegalArgumentException.class, () -> LittleEndian.putUint32(buf, 0, -1L));
        assertThrows(IllegalArgumentException.class, () -> LittleEndian.putUint32(buf, 0, 1L << 32));
        assertArrayEquals(new byte[8], buf);
    }

    @Test
    void accessOutsideTheArrayIsRejected() {
        byte[] buf = new byte[8];
        assertThrows(IndexOutOfBoundsException.class, () -> LittleEndian.putUint16(buf, 7, 1));
        assertThrows(IndexOutOfBoundsException.class, () -> LittleEndian.getUint16(buf, 7));
        assertThrows(IndexOutOfBoundsException.class, () -> LittleEndian.putUint32(buf, 5, 1));
        assertThrows(IndexOutOfBoundsException.class, () -> LittleEndian.getUint32(buf, 5));
        assertThrows(IndexOutOfBoundsException.class, () -> LittleEndian.putUint64(buf, 1, 1));
        assertThrows(IndexOutOfBoundsException.class, () -> LittleEndian.getUint64(buf, -1));
    }
}
