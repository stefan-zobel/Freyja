package io.github.stefan_zobel.freyja.common;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Fixed-width little-endian integer codecs over byte arrays.
 *
 * <p>Every fixed-width integer on disk or on the network is little-endian. The byte order is
 * stated explicitly here and never taken from the platform.
 *
 * <p>All methods throw {@link IndexOutOfBoundsException} if the value does not fit between
 * {@code offset} and the end of the array.
 */
public final class LittleEndian {

    private static final VarHandle INT16 =
            MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT32 =
            MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT64 =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    /** Largest value of an unsigned 16-bit integer. */
    public static final int UINT16_MAX = 0xFFFF;

    /** Largest value of an unsigned 32-bit integer. */
    public static final long UINT32_MAX = 0xFFFF_FFFFL;

    private LittleEndian() {
    }

    /**
     * Writes an unsigned 16-bit integer.
     *
     * @throws IllegalArgumentException if {@code value} is outside {@code 0..65535}
     */
    public static void putUint16(byte[] buf, int offset, int value) {
        if (value < 0 || value > UINT16_MAX) {
            throw new IllegalArgumentException("not a uint16: " + value);
        }
        INT16.set(buf, offset, (short) value);
    }

    /** Reads an unsigned 16-bit integer as a value in {@code 0..65535}. */
    public static int getUint16(byte[] buf, int offset) {
        return (short) INT16.get(buf, offset) & UINT16_MAX;
    }

    /**
     * Writes an unsigned 32-bit integer.
     *
     * @throws IllegalArgumentException if {@code value} is outside {@code 0..2^32-1}
     */
    public static void putUint32(byte[] buf, int offset, long value) {
        if (value < 0 || value > UINT32_MAX) {
            throw new IllegalArgumentException("not a uint32: " + value);
        }
        INT32.set(buf, offset, (int) value);
    }

    /** Reads an unsigned 32-bit integer as a value in {@code 0..2^32-1}. */
    public static long getUint32(byte[] buf, int offset) {
        return (int) INT32.get(buf, offset) & UINT32_MAX;
    }

    /** Writes the 32 bits of {@code value}. Used for bit patterns such as checksums. */
    public static void putInt32(byte[] buf, int offset, int value) {
        INT32.set(buf, offset, value);
    }

    /** Reads 32 bits. Used for bit patterns such as checksums. */
    public static int getInt32(byte[] buf, int offset) {
        return (int) INT32.get(buf, offset);
    }

    /**
     * Writes the 64 bits of {@code value}. A {@code long} holding an unsigned 64-bit integer
     * is written with the same bits.
     */
    public static void putUint64(byte[] buf, int offset, long value) {
        INT64.set(buf, offset, value);
    }

    /**
     * Reads 64 bits. If the stored value is an unsigned 64-bit integer above
     * {@code Long.MAX_VALUE}, the result is negative and must be handled with the unsigned
     * methods of {@link Long}.
     */
    public static long getUint64(byte[] buf, int offset) {
        return (long) INT64.get(buf, offset);
    }
}
