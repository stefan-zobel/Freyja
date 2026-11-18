package io.github.sz.freyja.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.CRC32C;

import org.junit.jupiter.api.Test;

/**
 * Published CRC32C test vectors and sensitivity of the context checksum
 * (docs/spec/verification.md, section 2.8).
 */
class ChecksumsTest {

    /** Bit-by-bit CRC32C (Castagnoli, reflected polynomial 0x82F63B78). */
    private static int referenceCrc32c(byte[]... parts) {
        int crc = ~0;
        for (byte[] part : parts) {
            for (byte b : part) {
                crc ^= Byte.toUnsignedInt(b);
                for (int bit = 0; bit < 8; bit++) {
                    crc = ((crc & 1) != 0) ? (crc >>> 1) ^ 0x82F63B78 : crc >>> 1;
                }
            }
        }
        return ~crc;
    }

    /** Hand-written little-endian encoding of a 64-bit value. */
    private static byte[] le64(long value) {
        byte[] out = new byte[8];
        for (int i = 0; i < 8; i++) {
            out[i] = (byte) (value >>> (8 * i));
        }
        return out;
    }

    private static int crc(byte[] data) {
        return Checksums.crc32c(data, 0, data.length);
    }

    @Test
    void publishedVectors() {
        // The standard check value.
        assertEquals(0xE3069283, crc("123456789".getBytes(StandardCharsets.US_ASCII)));
        assertEquals(0, crc(new byte[0]));

        // RFC 3720, appendix B.4.
        byte[] zeros = new byte[32];
        byte[] ones = new byte[32];
        Arrays.fill(ones, (byte) 0xFF);
        byte[] ascending = new byte[32];
        byte[] descending = new byte[32];
        for (int i = 0; i < 32; i++) {
            ascending[i] = (byte) i;
            descending[i] = (byte) (31 - i);
        }
        assertEquals(0x8A9136AA, crc(zeros));
        assertEquals(0x62A8AB43, crc(ones));
        assertEquals(0x46DD794E, crc(ascending));
        assertEquals(0x113FDB5C, crc(descending));
    }

    @Test
    void agreesWithTheBitwiseReference() {
        Random random = new Random(1);
        for (int length = 0; length <= 300; length++) {
            byte[] data = new byte[length];
            random.nextBytes(data);
            assertEquals(referenceCrc32c(data), crc(data), "length " + length);
        }
    }

    @Test
    void honorsOffsetAndLength() {
        byte[] data = "xx123456789yyy".getBytes(StandardCharsets.US_ASCII);
        assertEquals(0xE3069283, Checksums.crc32c(data, 2, 9));
        assertEquals(referenceCrc32c("123456789".getBytes(StandardCharsets.US_ASCII), le64(7), le64(4096)),
                Checksums.contextCrc32c(data, 2, 9, 7, 4096));
    }

    @Test
    void contextChecksumIsTheCrcOverDataThenFileNumberThenOffset() {
        Random random = new Random(2);
        long[] numbers = {0, 1, 255, 256, 1L << 32, Long.MAX_VALUE, -1L};
        for (int length = 0; length <= 64; length++) {
            byte[] data = new byte[length];
            random.nextBytes(data);
            for (long fileNumber : numbers) {
                for (long fileOffset : numbers) {
                    int expected = referenceCrc32c(data, le64(fileNumber), le64(fileOffset));
                    assertEquals(expected,
                            Checksums.contextCrc32c(data, 0, length, fileNumber, fileOffset));
                }
            }
        }
    }

    @Test
    void finishWithContextCoversUnitsMadeOfSeveralPieces() {
        byte[] header = {0x10, 0x00, 0x01};
        byte[] payload = "payload".getBytes(StandardCharsets.US_ASCII);
        CRC32C crc = new CRC32C();
        crc.update(header, 0, header.length);
        crc.update(payload, 0, payload.length);
        assertEquals(referenceCrc32c(header, payload, le64(12), le64(65536)),
                Checksums.finishWithContext(crc, 12, 65536));
    }

    @Test
    void everySingleBitChangeOfFileNumberOrOffsetChangesTheChecksum() {
        Random random = new Random(3);
        long[] bases = {0, 1, 42, 1L << 40, -1L, random.nextLong(), random.nextLong()};
        for (int length : new int[] {0, 1, 7, 100, 4096}) {
            byte[] data = new byte[length];
            random.nextBytes(data);
            for (long fileNumber : bases) {
                for (long fileOffset : bases) {
                    int original = Checksums.contextCrc32c(data, 0, length, fileNumber, fileOffset);
                    for (int bit = 0; bit < 64; bit++) {
                        long flipped = 1L << bit;
                        assertNotEquals(original, Checksums.contextCrc32c(
                                data, 0, length, fileNumber ^ flipped, fileOffset), "file number bit " + bit);
                        assertNotEquals(original, Checksums.contextCrc32c(
                                data, 0, length, fileNumber, fileOffset ^ flipped), "offset bit " + bit);
                    }
                }
            }
        }
    }

    @Test
    void swappingFileNumberAndOffsetChangesTheChecksum() {
        byte[] data = {1, 2, 3};
        assertNotEquals(Checksums.contextCrc32c(data, 0, 3, 5, 9),
                Checksums.contextCrc32c(data, 0, 3, 9, 5));
        assertNotEquals(Checksums.crc32c(data, 0, 3), Checksums.contextCrc32c(data, 0, 3, 0, 0));
    }
}
