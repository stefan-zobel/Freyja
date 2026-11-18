package io.github.sz.freyja.common;

import java.util.zip.CRC32C;
import java.util.zip.Checksum;

/**
 * CRC32C checksums, plain and bound to a storage location.
 *
 * <p>A context checksum is the CRC32C over the stored bytes followed by two values that are
 * not stored: the file number and the file offset at which the checksummed unit starts, each
 * as a little-endian {@code uint64}. A unit that ends up in the wrong file or at the wrong
 * offset therefore fails its checksum (docs/spec/storage-format.md, section 1).
 */
public final class Checksums {

    private static final int CONTEXT_LENGTH = 16;

    private Checksums() {
    }

    /** Returns the CRC32C of {@code length} bytes starting at {@code offset}. */
    public static int crc32c(byte[] buf, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(buf, offset, length);
        return (int) crc.getValue();
    }

    /**
     * Returns the context checksum of a unit that consists of one byte range.
     *
     * @param fileNumber number of the file that holds the unit
     * @param fileOffset offset in that file at which the unit starts
     */
    public static int contextCrc32c(byte[] buf, int offset, int length,
                                    long fileNumber, long fileOffset) {
        CRC32C crc = new CRC32C();
        crc.update(buf, offset, length);
        return finishWithContext(crc, fileNumber, fileOffset);
    }

    /**
     * Completes a context checksum for a unit whose stored bytes were already fed to
     * {@code crc}, in on-disk order. Use this when the unit consists of several pieces.
     *
     * @param crc a CRC32C that has consumed the stored bytes of the unit
     * @return the context checksum
     */
    public static int finishWithContext(Checksum crc, long fileNumber, long fileOffset) {
        byte[] context = new byte[CONTEXT_LENGTH];
        LittleEndian.putUint64(context, 0, fileNumber);
        LittleEndian.putUint64(context, 8, fileOffset);
        crc.update(context, 0, CONTEXT_LENGTH);
        return (int) crc.getValue();
    }
}
