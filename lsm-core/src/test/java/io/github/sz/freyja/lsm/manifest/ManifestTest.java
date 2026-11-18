package io.github.sz.freyja.lsm.manifest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.testing.Seeds;
import io.github.sz.freyja.lsm.CorruptionException;
import io.github.sz.freyja.lsm.UnsupportedFormatException;
import io.github.sz.freyja.lsm.fs.MemoryFileSystem;
import io.github.sz.freyja.lsm.fs.ReadableFile;
import io.github.sz.freyja.lsm.fs.WritableFile;
import io.github.sz.freyja.lsm.table.InternalKey;

/** Tests of version edits, versions, file names, and the CURRENT file. */
class ManifestTest {

    private static byte[] key(String userKey, long sequence) {
        return InternalKey.encode(userKey.getBytes(StandardCharsets.US_ASCII), sequence, InternalKey.TYPE_VALUE);
    }

    private static FileMeta file(long number, String smallest, String largest) {
        return new FileMeta(number, 1000 + number, key(smallest, 9), key(largest, 3), 3, 9);
    }

    // ---- version edits ---------------------------------------------------------------------

    @Test
    void anEditHasExactlyTheSpecifiedBytes() {
        byte[] smallest = key("a", 5);
        byte[] largest = key("b", 4);
        VersionEdit edit = new VersionEdit()
                .nextFileNumber(300)
                .lastSequence(7)
                .minWalNumber(2)
                .removeFile(1, 9)
                .addFile(0, new FileMeta(12, 204, smallest, largest, 4, 5));
        byte[] expected = {
            1, (byte) 0xAC, 0x02,     // tag 1: next file number 300
            2, 7,                     // tag 2: last sequence 7
            3, 2,                     // tag 3: min WAL number 2
            5, 1, 9,                  // tag 5: remove file 9 from level 1
            4, 0, 12, (byte) 0xCC, 0x01,  // tag 4: add to level 0 file 12 of 204 bytes
            10, 'a', -1, -1, -1, -1, -1, -1, -1, (byte) 0xFA, 1,
            10, 'b', -1, -1, -1, -1, -1, -1, -1, (byte) 0xFB, 1,
            4, 5,                     // smallest and largest sequence
        };
        assertArrayEquals(expected, edit.encode());
    }

    @Test
    void editsRoundTrip() {
        Seeds.forEach(ManifestTest.class, "version-edit", 300, seed -> {
            Random random = new Random(seed);
            VersionEdit edit = new VersionEdit();
            if (random.nextBoolean()) {
                edit.nextFileNumber(random.nextLong(Long.MAX_VALUE));
            }
            if (random.nextBoolean()) {
                edit.lastSequence(random.nextLong(Long.MAX_VALUE));
            }
            if (random.nextBoolean()) {
                edit.minWalNumber(random.nextLong(1_000_000));
            }
            for (int i = random.nextInt(4); i > 0; i--) {
                edit.removeFile(random.nextInt(7), random.nextLong(1_000_000));
            }
            for (int i = random.nextInt(4); i > 0; i--) {
                byte[] userKey = new byte[1 + random.nextInt(40)];
                random.nextBytes(userKey);
                long low = random.nextLong(1000);
                long high = low + 1 + random.nextLong(1000);
                edit.addFile(random.nextInt(7), new FileMeta(random.nextLong(1_000_000),
                        random.nextLong(Long.MAX_VALUE),
                        InternalKey.encode(userKey, high, InternalKey.TYPE_VALUE),
                        InternalKey.encode(userKey, low, InternalKey.TYPE_TOMBSTONE), low, high));
            }
            byte[] encoded = edit.encode();
            VersionEdit decoded = VersionEdit.decode(encoded);
            assertEquals(edit.nextFileNumber(), decoded.nextFileNumber());
            assertEquals(edit.lastSequence(), decoded.lastSequence());
            assertEquals(edit.minWalNumber(), decoded.minWalNumber());
            assertEquals(edit.removedFiles(), decoded.removedFiles());
            assertEquals(edit.addedFiles(), decoded.addedFiles());
            assertArrayEquals(encoded, decoded.encode());

            // Every proper prefix is either a shorter valid edit or rejected, never a crash.
            for (int length = 0; length < encoded.length; length++) {
                try {
                    VersionEdit.decode(java.util.Arrays.copyOf(encoded, length));
                } catch (DecodeException | VersionEdit.UnknownTagException expected) {
                    // Rejected.
                }
            }
        });
    }

    @Test
    void anEmptyEditIsValid() {
        VersionEdit decoded = VersionEdit.decode(new byte[0]);
        assertNull(decoded.nextFileNumber());
        assertNull(decoded.lastSequence());
        assertNull(decoded.minWalNumber());
        assertTrue(decoded.addedFiles().isEmpty());
        assertEquals(0, new VersionEdit().encode().length);
    }

    @Test
    void malformedEditsAreRejected() {
        // An unknown tag cannot be skipped.
        assertThrows(VersionEdit.UnknownTagException.class, () -> VersionEdit.decode(new byte[] {6, 1}));
        assertThrows(VersionEdit.UnknownTagException.class, () -> VersionEdit.decode(new byte[] {0}));
        // A field twice.
        assertThrows(DecodeException.class, () -> VersionEdit.decode(new byte[] {1, 5, 1, 6}));
        // A value is missing.
        assertThrows(DecodeException.class, () -> VersionEdit.decode(new byte[] {2}));
        // A key that is no internal key.
        assertThrows(DecodeException.class, () -> VersionEdit.decode(new byte[] {4, 0, 1, 1, 3, 1, 2, 3}));
        // A key length beyond the record.
        assertThrows(DecodeException.class, () -> VersionEdit.decode(new byte[] {4, 0, 1, 1, 100, 1}));
        // Smallest key after largest key.
        VersionEdit inverted = new VersionEdit().addFile(0,
                new FileMeta(1, 1, key("b", 1), key("a", 1), 1, 1));
        assertThrows(DecodeException.class, () -> VersionEdit.decode(inverted.encode()));
        assertThrows(IllegalArgumentException.class, () -> new VersionEdit().nextFileNumber(-1));
    }

    // ---- versions --------------------------------------------------------------------------

    @Test
    void aVersionAppliesEditsAndKeepsLevelZeroNewestFirst() {
        Version empty = Version.empty(3);
        assertEquals(3, empty.levelCount());
        assertEquals(1, empty.nextFileNumber());
        assertTrue(empty.allFiles().isEmpty());

        Version one = empty.apply(new VersionEdit().addFile(0, file(4, "a", "m")).nextFileNumber(5).lastSequence(9));
        Version two = one.apply(new VersionEdit().addFile(0, file(7, "c", "z")).addFile(1, file(6, "a", "b"))
                .nextFileNumber(8).minWalNumber(5));
        assertEquals(List.of(file(7, "c", "z"), file(4, "a", "m")), two.files(0));
        assertEquals(List.of(file(6, "a", "b")), two.files(1));
        assertEquals(8, two.nextFileNumber());
        assertEquals(9, two.lastSequence());
        assertEquals(5, two.minWalNumber());
        // Earlier versions are unchanged.
        assertEquals(List.of(file(4, "a", "m")), one.files(0));
        assertTrue(empty.files(0).isEmpty());

        Version three = two.apply(new VersionEdit().removeFile(0, 4));
        assertEquals(List.of(file(7, "c", "z")), three.files(0));
        assertEquals(2, three.allFiles().size());
        assertThrows(UnsupportedOperationException.class, () -> three.files(0).add(file(99, "a", "b")));
    }

    @Test
    void aSnapshotEditReproducesTheVersion() {
        Version version = Version.empty(4)
                .apply(new VersionEdit().addFile(0, file(4, "a", "m")).addFile(0, file(9, "b", "c"))
                        .addFile(2, file(5, "x", "y")).nextFileNumber(10).lastSequence(77).minWalNumber(8));
        Version rebuilt = Version.empty(4).apply(VersionEdit.decode(version.toEdit().encode()));
        for (int level = 0; level < 4; level++) {
            assertEquals(version.files(level), rebuilt.files(level));
        }
        assertEquals(10, rebuilt.nextFileNumber());
        assertEquals(77, rebuilt.lastSequence());
        assertEquals(8, rebuilt.minWalNumber());
    }

    @Test
    void editsThatContradictTheVersionAreRejected() {
        Version version = Version.empty(2)
                .apply(new VersionEdit().addFile(0, file(4, "a", "m")).nextFileNumber(5).lastSequence(9).minWalNumber(3));
        assertThrows(DecodeException.class, () -> version.apply(new VersionEdit().addFile(0, file(4, "a", "m"))));
        assertThrows(DecodeException.class, () -> version.apply(new VersionEdit().addFile(1, file(4, "a", "m"))));
        assertThrows(DecodeException.class, () -> version.apply(new VersionEdit().removeFile(0, 5)));
        assertThrows(DecodeException.class, () -> version.apply(new VersionEdit().removeFile(1, 4)));
        assertThrows(DecodeException.class, () -> version.apply(new VersionEdit().addFile(2, file(6, "a", "m"))));
        assertThrows(DecodeException.class, () -> version.apply(new VersionEdit().nextFileNumber(4)));
        assertThrows(DecodeException.class, () -> version.apply(new VersionEdit().lastSequence(8)));
        assertThrows(DecodeException.class, () -> version.apply(new VersionEdit().minWalNumber(2)));
        // Removing and adding the same file number in one edit moves the file.
        Version moved = version.apply(new VersionEdit().removeFile(0, 4).addFile(1, file(4, "a", "m")));
        assertEquals(List.of(file(4, "a", "m")), moved.files(1));
    }

    @Test
    void aFileKnowsWhichUserKeysItsRangeIncludes() {
        FileMeta meta = file(1, "b", "d");
        assertFalse(meta.mayContainUserKey("a".getBytes(StandardCharsets.US_ASCII)));
        assertTrue(meta.mayContainUserKey("b".getBytes(StandardCharsets.US_ASCII)));
        assertTrue(meta.mayContainUserKey("bzz".getBytes(StandardCharsets.US_ASCII)));
        assertTrue(meta.mayContainUserKey("d".getBytes(StandardCharsets.US_ASCII)));
        assertFalse(meta.mayContainUserKey("da".getBytes(StandardCharsets.US_ASCII)));
    }

    // ---- file names ------------------------------------------------------------------------

    @Test
    void fileNamesAreBuiltAndParsed() {
        Path dir = Path.of("db");
        assertEquals("000042.wal", FileNames.wal(dir, 42).getFileName().toString());
        assertEquals("000042.sst", FileNames.table(dir, 42).getFileName().toString());
        assertEquals("MANIFEST-000042", FileNames.manifest(dir, 42).getFileName().toString());
        assertEquals("1234567.sst", FileNames.table(dir, 1234567).getFileName().toString());

        assertEquals(new FileNames.Numbered(FileNames.Kind.WAL, 42), FileNames.parse(FileNames.wal(dir, 42)));
        assertEquals(new FileNames.Numbered(FileNames.Kind.TABLE, 1234567), FileNames.parse("1234567.sst"));
        assertEquals(new FileNames.Numbered(FileNames.Kind.MANIFEST, 7), FileNames.parse("MANIFEST-000007"));
        for (String other : new String[] {"CURRENT", "LOCK", "CURRENT.tmp", "x1.wal", ".sst", "MANIFEST-",
                "MANIFEST-12a", "000001.sst.tmp", "-1.wal", "1234567890123456789.wal"}) {
            assertNull(FileNames.parse(other), other);
        }
    }

    // ---- CURRENT ---------------------------------------------------------------------------

    private final MemoryFileSystem fs = new MemoryFileSystem();
    private final Path dir = Path.of("manifest-test").toAbsolutePath();

    private void writeCurrent(byte[] content) throws IOException {
        fs.createDirectories(dir);
        Path path = dir.resolve(FileNames.CURRENT);
        if (fs.exists(path)) {
            fs.delete(path);
        }
        try (WritableFile file = fs.create(path)) {
            file.append(content, 0, content.length);
        }
    }

    @Test
    void theCurrentFileHasExactlyTheSpecifiedText() throws IOException {
        fs.createDirectories(dir);
        CurrentFile.write(fs, dir, 5);
        byte[] bytes;
        try (ReadableFile file = fs.open(dir.resolve(FileNames.CURRENT))) {
            bytes = new byte[(int) file.size()];
            file.readFully(0, bytes, 0, bytes.length);
        }
        String text = new String(bytes, StandardCharsets.US_ASCII);
        assertTrue(text.startsWith("FREYJA CURRENT 1\nMANIFEST-000005\ncrc32c="), text);
        assertEquals("FREYJA CURRENT 1\nMANIFEST-000005\n".length() + "crc32c=".length() + 8 + 1, text.length());
        assertTrue(text.endsWith("\n"));
        assertEquals(5, CurrentFile.read(fs, dir));
        assertFalse(fs.exists(dir.resolve(FileNames.CURRENT_TMP)));
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void replacingCurrentIsAtomicAndDurable() throws IOException {
        fs.createDirectories(dir);
        CurrentFile.write(fs, dir, 5);
        CurrentFile.write(fs, dir, 9);
        assertEquals(9, CurrentFile.read(fs, dir));
        fs.crash();
        assertEquals(9, CurrentFile.read(fs, dir));

        // A leftover temporary file from an interrupted replace does not get in the way.
        try (WritableFile file = fs.create(dir.resolve(FileNames.CURRENT_TMP))) {
            file.append(new byte[] {1}, 0, 1);
        }
        CurrentFile.write(fs, dir, 11);
        assertEquals(11, CurrentFile.read(fs, dir));
    }

    @Test
    void everySingleBitFlipOfCurrentIsDetected() throws IOException {
        byte[] good = CurrentFile.encode("MANIFEST-000005");
        for (int bit = 0; bit < good.length * 8; bit++) {
            byte[] damaged = good.clone();
            damaged[bit >>> 3] ^= (byte) (1 << (bit & 7));
            writeCurrent(damaged);
            int flipped = bit;
            assertThrows(CorruptionException.class, () -> CurrentFile.read(fs, dir), () -> "bit " + flipped);
        }
        for (int length = 0; length < good.length; length++) {
            writeCurrent(java.util.Arrays.copyOf(good, length));
            assertThrows(CorruptionException.class, () -> CurrentFile.read(fs, dir));
        }
        writeCurrent(new byte[1000]);
        assertThrows(CorruptionException.class, () -> CurrentFile.read(fs, dir));
    }

    @Test
    void anUnknownCurrentVersionOrABadNameIsRejected() throws IOException {
        // Intact content with another version is unsupported, not damaged.
        String head = "FREYJA CURRENT 2\nMANIFEST-000005\n";
        byte[] headBytes = head.getBytes(StandardCharsets.US_ASCII);
        int crc = io.github.sz.freyja.common.Checksums.crc32c(headBytes, 0, headBytes.length);
        writeCurrent((head + String.format("crc32c=%08x\n", crc)).getBytes(StandardCharsets.US_ASCII));
        assertThrows(UnsupportedFormatException.class, () -> CurrentFile.read(fs, dir));

        // Intact content that does not name a manifest.
        for (String name : new String[] {"000005.sst", "MANIFEST-5", "MANIFEST-", "../MANIFEST-000005"}) {
            writeCurrent(CurrentFile.encode(name));
            assertThrows(CorruptionException.class, () -> CurrentFile.read(fs, dir), name);
        }
    }
}
