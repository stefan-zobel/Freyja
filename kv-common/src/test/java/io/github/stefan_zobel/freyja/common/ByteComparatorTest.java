package io.github.stefan_zobel.freyja.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Exhaustive check of the unsigned byte comparator over all byte strings of up to 3 bytes
 * from a boundary alphabet (docs/spec/verification.md, section 2.8).
 */
class ByteComparatorTest {

    private static final int[] ALPHABET = {0x00, 0x01, 0x7F, 0x80, 0xFF};

    /** All strings of length 0 to 3 over the alphabet: 1 + 5 + 25 + 125 = 156. */
    private static List<byte[]> allStrings() {
        List<byte[]> strings = new ArrayList<>();
        strings.add(new byte[0]);
        for (int a : ALPHABET) {
            strings.add(new byte[] {(byte) a});
            for (int b : ALPHABET) {
                strings.add(new byte[] {(byte) a, (byte) b});
                for (int c : ALPHABET) {
                    strings.add(new byte[] {(byte) a, (byte) b, (byte) c});
                }
            }
        }
        return strings;
    }

    /** Hand-written reference: first differing byte decides, as a number 0..255; then length. */
    private static int reference(byte[] a, byte[] b) {
        for (int i = 0; i < a.length && i < b.length; i++) {
            int x = a[i] < 0 ? a[i] + 256 : a[i];
            int y = b[i] < 0 ? b[i] + 256 : b[i];
            if (x < y) {
                return -1;
            }
            if (x > y) {
                return 1;
            }
        }
        if (a.length < b.length) {
            return -1;
        }
        return a.length > b.length ? 1 : 0;
    }

    private static String hex(byte[] bytes) {
        return "[" + HexFormat.of().formatHex(bytes) + "]";
    }

    @Test
    void agreesWithTheReferenceAndIsAntisymmetricForEveryPair() {
        List<byte[]> strings = allStrings();
        assertEquals(156, strings.size());
        for (byte[] a : strings) {
            for (byte[] b : strings) {
                int expected = reference(a, b);
                int actual = Integer.signum(ByteComparator.compare(a, b));
                if (actual != expected) {
                    fail("compare(" + hex(a) + ", " + hex(b) + ") = " + actual);
                }
                if (Integer.signum(ByteComparator.compare(b, a)) != -actual) {
                    fail("not antisymmetric for " + hex(a) + ", " + hex(b));
                }
                if ((actual == 0) != Arrays.equals(a, b)) {
                    fail("zero does not mean equal for " + hex(a) + ", " + hex(b));
                }
                if (Integer.signum(ByteComparator.UNSIGNED.compare(a, b)) != expected) {
                    fail("comparator constant disagrees for " + hex(a) + ", " + hex(b));
                }
            }
        }
    }

    @Test
    void aProperPrefixSortsFirst() {
        for (byte[] longer : allStrings()) {
            for (int length = 0; length < longer.length; length++) {
                byte[] prefix = Arrays.copyOf(longer, length);
                assertTrue(ByteComparator.compare(prefix, longer) < 0,
                        hex(prefix) + " must sort before " + hex(longer));
            }
        }
    }

    @Test
    void isTransitiveForEveryTriple() {
        List<byte[]> strings = allStrings();
        int n = strings.size();
        boolean[][] lessOrEqual = new boolean[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                lessOrEqual[i][j] = ByteComparator.compare(strings.get(i), strings.get(j)) <= 0;
            }
        }
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                if (!lessOrEqual[i][j]) {
                    continue;
                }
                for (int k = 0; k < n; k++) {
                    if (lessOrEqual[j][k] && !lessOrEqual[i][k]) {
                        fail("not transitive: " + hex(strings.get(i)) + ", "
                                + hex(strings.get(j)) + ", " + hex(strings.get(k)));
                    }
                }
            }
        }
    }

    @Test
    void bytesAreUnsigned() {
        // A signed comparison would put 0x80 and 0xFF before 0x7F.
        assertTrue(ByteComparator.compare(new byte[] {0x7F}, new byte[] {(byte) 0x80}) < 0);
        assertTrue(ByteComparator.compare(new byte[] {(byte) 0x80}, new byte[] {(byte) 0xFF}) < 0);
        assertTrue(ByteComparator.compare(new byte[] {0x00}, new byte[] {(byte) 0xFF}) < 0);
    }

    @Test
    void rangeVariantAgreesWithTheReferenceForEveryPair() {
        List<byte[]> strings = allStrings();
        for (byte[] a : strings) {
            for (byte[] b : strings) {
                // Embed both strings in larger arrays with different surroundings.
                byte[] left = new byte[a.length + 3];
                byte[] right = new byte[b.length + 5];
                Arrays.fill(left, (byte) 0xEE);
                Arrays.fill(right, (byte) 0x11);
                System.arraycopy(a, 0, left, 2, a.length);
                System.arraycopy(b, 0, right, 1, b.length);
                int actual = Integer.signum(
                        ByteComparator.compare(left, 2, a.length, right, 1, b.length));
                if (actual != reference(a, b)) {
                    fail("range compare(" + hex(a) + ", " + hex(b) + ") = " + actual);
                }
            }
        }
    }
}
