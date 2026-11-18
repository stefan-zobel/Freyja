package io.github.sz.freyja.node.store;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import io.github.sz.freyja.common.Checksums;
import io.github.sz.freyja.common.LittleEndian;
import io.github.sz.freyja.lsm.UnsupportedFormatException;
import io.github.sz.freyja.lsm.fs.ReadableFile;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.lsm.fs.WritableFile;

/**
 * A small file that holds one checksummed record and is replaced as a whole.
 *
 * <pre>
 * magic        uint32   identifies the kind of file
 * version      uint8    format version
 * body         bytes    fixed length per kind and version, or of any length up to 4 KiB
 * checksum     uint32   CRC32C over everything before it, bound to the file and to offset 0
 * </pre>
 *
 * <p>All integers are little-endian. The checksum also covers the number that stands for the
 * file, so the content of one file is not valid in another.
 */
final class SmallFile {

    private static final int HEAD = 5;
    private static final int CHECKSUM = 4;
    /** For {@link #read}: the body may have any length. */
    static final int ANY_LENGTH = -1;

    private SmallFile() {
    }

    /** Builds the stored bytes of a record. */
    static byte[] encode(long magic, int version, byte[] body, long fileNumber) {
        byte[] bytes = new byte[HEAD + body.length + CHECKSUM];
        LittleEndian.putUint32(bytes, 0, magic);
        bytes[4] = (byte) version;
        System.arraycopy(body, 0, bytes, HEAD, body.length);
        int crc = Checksums.contextCrc32c(bytes, 0, HEAD + body.length, fileNumber, 0);
        LittleEndian.putInt32(bytes, HEAD + body.length, crc);
        return bytes;
    }

    /**
     * Reads the body of the record in {@code path}.
     *
     * @param bodyLength the length the body must have, or {@link #ANY_LENGTH}
     * @return the body, or {@code null} if the file does not exist or does not hold a valid
     *         record of this kind
     * @throws UnsupportedFormatException if the record is valid but of another format version
     */
    static byte[] read(StorageFileSystem fs, Path path, long magic, int version, int bodyLength,
                       long fileNumber) throws IOException {
        byte[] bytes;
        try (ReadableFile file = fs.open(path)) {
            long size = file.size();
            if (size < HEAD + CHECKSUM || size > 4096) {
                return null;
            }
            bytes = new byte[(int) size];
            file.readFully(0, bytes, 0, bytes.length);
        } catch (NoSuchFileException e) {
            return null;
        }
        int end = bytes.length - CHECKSUM;
        if (LittleEndian.getUint32(bytes, 0) != magic
                || LittleEndian.getInt32(bytes, end) != Checksums.contextCrc32c(bytes, 0, end, fileNumber, 0)) {
            return null;
        }
        if ((bytes[4] & 0xFF) != version) {
            throw new UnsupportedFormatException(path.getFileName() + " has format version "
                    + (bytes[4] & 0xFF) + "; this software reads version " + version);
        }
        if (bodyLength != ANY_LENGTH && end - HEAD != bodyLength) {
            return null;
        }
        byte[] body = new byte[end - HEAD];
        System.arraycopy(bytes, HEAD, body, 0, body.length);
        return body;
    }

    /**
     * Replaces {@code path} by a file with {@code bytes}: the bytes are written to a temporary
     * file and synced, the file is renamed over {@code path}, and the directory is synced.
     * After a crash the file holds either its old content or the new one.
     */
    static void replace(StorageFileSystem fs, Path path, byte[] bytes) throws IOException {
        Path temporary = temporaryOf(path);
        if (fs.exists(temporary)) {
            fs.delete(temporary);
        }
        try (WritableFile file = fs.create(temporary)) {
            file.append(bytes, 0, bytes.length);
            file.sync();
        }
        fs.rename(temporary, path);
        fs.syncDirectory(path.getParent());
    }

    static Path temporaryOf(Path path) {
        return path.resolveSibling(path.getFileName() + ".tmp");
    }
}
