package io.github.sz.freyja.lsm.manifest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import io.github.sz.freyja.common.Checksums;
import io.github.sz.freyja.lsm.CorruptionException;
import io.github.sz.freyja.lsm.UnsupportedFormatException;
import io.github.sz.freyja.lsm.fs.ReadableFile;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.lsm.fs.WritableFile;

/**
 * The {@code CURRENT} file, which names the active manifest
 * (docs/spec/storage-format.md, section 6).
 *
 * <p>The file is ASCII text of three lines, each ended by a line feed:
 * <pre>
 * FREYJA CURRENT 1
 * MANIFEST-000005
 * crc32c=1a2b3c4d
 * </pre>
 * The first line is the magic text and the format version. The checksum is the CRC32C of the
 * first two lines including their line feeds, as eight lowercase hexadecimal digits.
 */
public final class CurrentFile {

    private static final String MAGIC = "FREYJA CURRENT ";
    private static final int FORMAT_VERSION = 1;
    private static final String CHECKSUM_PREFIX = "crc32c=";
    private static final int MAX_LENGTH = 256;

    private CurrentFile() {
    }

    /** Returns the content of a {@code CURRENT} file that names {@code manifestName}. */
    public static byte[] encode(String manifestName) {
        String head = MAGIC + FORMAT_VERSION + "\n" + manifestName + "\n";
        byte[] headBytes = head.getBytes(StandardCharsets.US_ASCII);
        int crc = Checksums.crc32c(headBytes, 0, headBytes.length);
        return (head + CHECKSUM_PREFIX + String.format("%08x", crc) + "\n").getBytes(StandardCharsets.US_ASCII);
    }

    /**
     * Reads {@code dir/CURRENT} and returns the manifest file number it names.
     *
     * @throws CorruptionException        if the file is damaged
     * @throws UnsupportedFormatException if it has an unknown format version
     */
    public static long read(StorageFileSystem fs, Path dir) throws IOException {
        Path path = dir.resolve(FileNames.CURRENT);
        String name = path.toString();
        byte[] bytes;
        try (ReadableFile file = fs.open(path)) {
            long size = file.size();
            if (size > MAX_LENGTH) {
                throw new CorruptionException(name, 0, "implausible size " + size);
            }
            bytes = new byte[(int) size];
            if (file.read(0, ByteBuffer.wrap(bytes)) != bytes.length) {
                throw new IOException("short read of " + name);
            }
        }
        String text = new String(bytes, StandardCharsets.ISO_8859_1);
        String[] lines = text.split("\n", -1);
        // Three lines, each ended by a line feed, give four parts with an empty last one.
        if (lines.length != 4 || !lines[3].isEmpty() || !lines[2].startsWith(CHECKSUM_PREFIX)) {
            throw new CorruptionException(name, 0, "not a CURRENT file");
        }
        String head = lines[0] + "\n" + lines[1] + "\n";
        byte[] headBytes = head.getBytes(StandardCharsets.ISO_8859_1);
        String expected = String.format("%08x", Checksums.crc32c(headBytes, 0, headBytes.length));
        if (!lines[2].equals(CHECKSUM_PREFIX + expected)) {
            throw new CorruptionException(name, 0, "checksum mismatch");
        }
        if (!lines[0].startsWith(MAGIC)) {
            throw new CorruptionException(name, 0, "not a CURRENT file");
        }
        if (!lines[0].equals(MAGIC + FORMAT_VERSION)) {
            throw new UnsupportedFormatException("CURRENT file " + name + " has the header '"
                    + lines[0] + "'; this release reads version " + FORMAT_VERSION);
        }
        FileNames.Numbered manifest = FileNames.parse(lines[1]);
        if (manifest == null || manifest.kind() != FileNames.Kind.MANIFEST
                || !FileNames.manifestName(manifest.number()).equals(lines[1])) {
            throw new CorruptionException(name, 0, "'" + lines[1] + "' is not a manifest name");
        }
        return manifest.number();
    }

    /**
     * Makes {@code dir/CURRENT} name the manifest with this number, by the atomic-replace
     * procedure: write a temporary file, sync it, close it, rename it over {@code CURRENT},
     * sync the directory (docs/spec/storage-engine.md, section 2).
     */
    public static void write(StorageFileSystem fs, Path dir, long manifestNumber) throws IOException {
        Path tmp = dir.resolve(FileNames.CURRENT_TMP);
        if (fs.exists(tmp)) {
            fs.delete(tmp);
        }
        byte[] content = encode(FileNames.manifestName(manifestNumber));
        try (WritableFile file = fs.create(tmp)) {
            file.append(content, 0, content.length);
            file.sync();
        }
        fs.rename(tmp, dir.resolve(FileNames.CURRENT));
        fs.syncDirectory(dir);
    }
}
