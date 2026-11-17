package io.github.stefan_zobel.freyja.common;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Lexicographic comparison of byte strings with every byte treated as unsigned
 * ({@code 0x00..0xFF}). A string that is a proper prefix of another sorts first.
 *
 * <p>This is the order of user keys everywhere in the system.
 */
public final class ByteComparator {

    /** The unsigned lexicographic order as a {@link Comparator}. */
    public static final Comparator<byte[]> UNSIGNED = ByteComparator::compare;

    private ByteComparator() {
    }

    /** Returns a negative value, zero, or a positive value if {@code a} is less than, equal to, or greater than {@code b}. */
    public static int compare(byte[] a, byte[] b) {
        return Arrays.compareUnsigned(a, b);
    }

    /**
     * Compares the range of {@code aLength} bytes at {@code aOffset} in {@code a} with the
     * range of {@code bLength} bytes at {@code bOffset} in {@code b}.
     */
    public static int compare(byte[] a, int aOffset, int aLength,
                              byte[] b, int bOffset, int bLength) {
        return Arrays.compareUnsigned(a, aOffset, aOffset + aLength, b, bOffset, bOffset + bLength);
    }
}
