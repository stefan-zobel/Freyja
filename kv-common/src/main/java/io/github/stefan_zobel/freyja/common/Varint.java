package io.github.stefan_zobel.freyja.common;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * Unsigned LEB128 varints for 64-bit values.
 *
 * <p>Each byte carries 7 value bits, least significant group first. The high bit of a byte is
 * set when more bytes follow. A {@code long} is treated as an unsigned 64-bit integer.
 *
 * <p>The decoder accepts exactly one encoding per value, the shortest one. It rejects
 * truncated input, encodings with redundant trailing zero groups, and encodings that do not
 * fit in 64 bits. The number of bytes consumed by a successful decode therefore always equals
 * {@link #encodedLength(long)} of the decoded value.
 */
public final class Varint {

    /** Largest number of bytes an encoded value can occupy. */
    public static final int MAX_LENGTH = 10;

    /** Result of {@link #decodedLength}: the input ends before the varint does. */
    public static final int TRUNCATED = -1;

    /** Result of {@link #decodedLength}: the encoding is longer than necessary. */
    public static final int OVERLONG = -2;

    /** Result of {@link #decodedLength}: the encoded value does not fit in 64 bits. */
    public static final int OVERFLOW = -3;

    private Varint() {
    }

    /** Returns the number of bytes in the encoding of {@code value}, between 1 and 10. */
    public static int encodedLength(long value) {
        int significantBits = Long.SIZE - Long.numberOfLeadingZeros(value | 1);
        return (significantBits + 6) / 7;
    }

    /**
     * Writes {@code value} at {@code offset}.
     *
     * @return the offset after the last byte written
     * @throws IndexOutOfBoundsException if the encoding does not fit; nothing is written then
     */
    public static int putUint64(byte[] buf, int offset, long value) {
        Objects.checkFromIndexSize(offset, encodedLength(value), buf.length);
        int pos = offset;
        while ((value & ~0x7FL) != 0) {
            buf[pos++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf[pos++] = (byte) value;
        return pos;
    }

    /**
     * Reads the varint that starts at {@code offset} and ends before {@code limit}.
     *
     * <p>The next value starts at {@code offset + encodedLength(result)}.
     *
     * @throws DecodeException if the bytes are truncated, overlong, or overflow 64 bits
     * @throws IndexOutOfBoundsException if {@code offset..limit} is not a range of {@code buf}
     */
    public static long getUint64(byte[] buf, int offset, int limit) {
        Objects.checkFromToIndex(offset, limit, buf.length);
        long result = 0;
        int pos = offset;
        for (int shift = 0; ; shift += 7) {
            if (pos >= limit) {
                throw new DecodeException("truncated varint at offset " + offset);
            }
            int b = buf[pos++] & 0xFF;
            if (shift == 63 && b > 1) {
                throw new DecodeException("varint overflows 64 bits at offset " + offset);
            }
            result |= (long) (b & 0x7F) << shift;
            if (b < 0x80) {
                if (b == 0 && shift > 0) {
                    throw new DecodeException("overlong varint at offset " + offset);
                }
                return result;
            }
        }
    }

    /**
     * Reads the varint that starts at {@code offset} of {@code segment} and ends before
     * {@code limit}. Same rules as {@link #getUint64(byte[], int, int)}.
     *
     * @throws DecodeException if the bytes are truncated, overlong, or overflow 64 bits
     * @throws IndexOutOfBoundsException if {@code offset..limit} is not a range of the segment
     */
    public static long getUint64(MemorySegment segment, long offset, long limit) {
        Objects.checkFromToIndex(offset, limit, segment.byteSize());
        long result = 0;
        long pos = offset;
        for (int shift = 0; ; shift += 7) {
            if (pos >= limit) {
                throw new DecodeException("truncated varint at offset " + offset);
            }
            int b = segment.get(ValueLayout.JAVA_BYTE, pos++) & 0xFF;
            if (shift == 63 && b > 1) {
                throw new DecodeException("varint overflows 64 bits at offset " + offset);
            }
            result |= (long) (b & 0x7F) << shift;
            if (b < 0x80) {
                if (b == 0 && shift > 0) {
                    throw new DecodeException("overlong varint at offset " + offset);
                }
                return result;
            }
        }
    }

    /**
     * Checks the varint that starts at {@code offset} and ends before {@code limit} without
     * throwing on invalid input.
     *
     * @return the length of the valid encoding (1 to 10), or one of {@link #TRUNCATED},
     *         {@link #OVERLONG}, {@link #OVERFLOW}
     * @throws IndexOutOfBoundsException if {@code offset..limit} is not a range of {@code buf}
     */
    public static int decodedLength(byte[] buf, int offset, int limit) {
        Objects.checkFromToIndex(offset, limit, buf.length);
        int pos = offset;
        for (int shift = 0; ; shift += 7) {
            if (pos >= limit) {
                return TRUNCATED;
            }
            int b = buf[pos++] & 0xFF;
            if (shift == 63 && b > 1) {
                return OVERFLOW;
            }
            if (b < 0x80) {
                if (b == 0 && shift > 0) {
                    return OVERLONG;
                }
                return pos - offset;
            }
        }
    }
}
