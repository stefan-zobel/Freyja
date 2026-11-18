package io.github.sz.freyja.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

/**
 * Compares the key hash with values produced by the XXH64 reference implementation
 * (xxHash 0.8.4, seed 0). The hash is part of the on-disk format, so these values must
 * never change.
 */
class KeyHashTest {

    private static long hashAscii(String text) {
        return KeyHash.hash(text.getBytes(StandardCharsets.US_ASCII));
    }

    /** Input of the length vectors: byte i is (7 * i + 3) mod 256. */
    private static byte[] pattern(int length) {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = (byte) (i * 7 + 3);
        }
        return data;
    }

    @Test
    void textVectors() {
        assertEquals(0xEF46DB3751D8E999L, hashAscii(""));
        assertEquals(0xD24EC4F1A98C6E5BL, hashAscii("a"));
        assertEquals(0x44BC2CF5AD770999L, hashAscii("abc"));
        assertEquals(0x8CB841DB40E6AE83L, hashAscii("123456789"));
    }

    @Test
    void vectorsForEveryCodePathByLength() {
        long[][] vectors = {
            {0, 0xEF46DB3751D8E999L},
            {1, 0x1F25C8D0BC1F4BB6L},
            {2, 0xF5BEDEC232706303L},
            {3, 0x31D2363F52E564C9L},
            {4, 0x9BB64B7D66EE9FDAL},
            {5, 0xC7608EFDDB7051FEL},
            {7, 0x9A7B149959CE60D8L},
            {8, 0xDAB99D95C6F90092L},
            {9, 0x170BB6BF975B4C02L},
            {15, 0x1B47CB8243CC8E32L},
            {16, 0x434850232B787BE2L},
            {17, 0x1EFA7025F1B97A7AL},
            {24, 0xC47C71BD4694FF81L},
            {31, 0xA2AA5F33CC4A6119L},
            {32, 0x23C3C17EF790FD97L},
            {33, 0x50A7CFC7BA588784L},
            {47, 0xA7C5465CC5AD205EL},
            {63, 0x5E3E54B431C7493CL},
            {64, 0x0EB64B3EF6EEB01FL},
            {65, 0xA383B724B2BD12F1L},
            {100, 0xA61F8D4C170FE531L},
            {255, 0x39AE55A29989206FL},
            {256, 0x00CFC5207DD8E201L},
            {1000, 0x5F235FA033F1A3FBL},
        };
        for (long[] vector : vectors) {
            int length = (int) vector[0];
            assertEquals(vector[1], KeyHash.hash(pattern(length)), "length " + length);
        }
    }

    @Test
    void honorsOffsetAndLength() {
        byte[] padded = new byte[100 + 5];
        System.arraycopy(pattern(100), 0, padded, 3, 100);
        assertEquals(0xA61F8D4C170FE531L, KeyHash.hash(padded, 3, 100));
        assertEquals(0xEF46DB3751D8E999L, KeyHash.hash(padded, 50, 0));
    }
}
