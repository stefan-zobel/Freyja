package io.github.stefan_zobel.freyja.lsm.table;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.common.ByteComparator;
import io.github.stefan_zobel.freyja.common.KeyHash;
import io.github.stefan_zobel.freyja.common.testing.Seeds;
import io.github.stefan_zobel.freyja.lsm.CorruptionException;
import io.github.stefan_zobel.freyja.lsm.UnsupportedFormatException;
import io.github.stefan_zobel.freyja.lsm.fs.MemoryFileSystem;
import io.github.stefan_zobel.freyja.lsm.fs.ReadableFile;
import io.github.stefan_zobel.freyja.lsm.fs.WritableFile;
import io.github.stefan_zobel.freyja.lsm.table.SSTableReader.TableIterator;
import io.github.stefan_zobel.freyja.lsm.table.SSTableWriter.TableInfo;

/**
 * Format and behavior tests of the table writer and reader, on the in-memory file system.
 */
class SSTableTest {

    private static final long FILE_NUMBER = 7;

    private final MemoryFileSystem fs = new MemoryFileSystem();
    private final Path dir = Path.of("sstable-test").toAbsolutePath();
    private int nextName;

    @BeforeEach
    void createDirectory() throws IOException {
        fs.createDirectories(dir);
    }

    // ---- helpers ---------------------------------------------------------------------------

    private record Entry(byte[] key, byte[] value) {
    }

    private Path freshPath() {
        return dir.resolve("table-" + (nextName++) + ".sst");
    }

    private static byte[] ascii(String text) {
        return text.getBytes(StandardCharsets.US_ASCII);
    }

    private static Entry put(String userKey, long sequence, String value) {
        return new Entry(InternalKey.encode(ascii(userKey), sequence, InternalKey.TYPE_VALUE), ascii(value));
    }

    private static Entry tombstone(String userKey, long sequence) {
        return new Entry(InternalKey.encode(ascii(userKey), sequence, InternalKey.TYPE_TOMBSTONE), new byte[0]);
    }

    private TableInfo writeTable(Path path, long fileNumber, List<Entry> entries) throws IOException {
        try (SSTableWriter writer = new SSTableWriter(fs, path, fileNumber)) {
            for (Entry entry : entries) {
                writer.add(entry.key(), entry.value());
            }
            return writer.finish();
        }
    }

    private byte[] readAll(Path path) throws IOException {
        try (ReadableFile file = fs.open(path)) {
            byte[] bytes = new byte[(int) file.size()];
            file.readFully(0, bytes, 0, bytes.length);
            return bytes;
        }
    }

    private Path writeBytes(byte[] bytes) throws IOException {
        Path path = freshPath();
        try (WritableFile file = fs.create(path)) {
            file.append(bytes, 0, bytes.length);
            file.sync();
        }
        return path;
    }

    /** Reads every entry through an iterator. */
    private static List<Entry> scan(SSTableReader reader) throws IOException {
        List<Entry> entries = new ArrayList<>();
        try (TableIterator it = reader.iterator()) {
            for (it.seekToFirst(); it.valid(); it.next()) {
                entries.add(new Entry(it.key(), it.value()));
            }
            // Backward, the table holds the same entries.
            List<Entry> backward = new ArrayList<>();
            for (it.seekToLast(); it.valid(); it.prev()) {
                backward.add(new Entry(it.key(), it.value()));
            }
            assertSameEntries(entries, backward.reversed());
            // Before an entry lies the entry before it, also across the border of a block.
            int step = Math.max(1, entries.size() / 64);
            for (int i = 0; i < entries.size(); i += step) {
                it.seekBefore(entries.get(i).key());
                if (i == 0) {
                    assertFalse(it.valid(), "nothing is before the first entry");
                } else {
                    assertTrue(it.valid(), "before entry " + i);
                    assertArrayEquals(entries.get(i - 1).key(), it.key(), "before entry " + i);
                    assertArrayEquals(entries.get(i - 1).value(), it.value(), "before entry " + i);
                }
            }
            if (!entries.isEmpty()) {
                byte[] beyond = InternalKey.encode(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                        (byte) 0xFF, (byte) 0xFF}, 0, InternalKey.TYPE_VALUE);
                it.seekBefore(beyond);
                assertTrue(it.valid());
                assertArrayEquals(entries.getLast().key(), it.key(), "before a key beyond the table");
            }
        }
        return entries;
    }

    private static void assertSameEntries(List<Entry> expected, List<Entry> actual) {
        assertEquals(expected.size(), actual.size(), "entry count");
        for (int i = 0; i < expected.size(); i++) {
            if (!Arrays.equals(expected.get(i).key(), actual.get(i).key())
                    || !Arrays.equals(expected.get(i).value(), actual.get(i).value())) {
                fail("entry " + i + " differs");
            }
        }
    }

    /** Entries with values large enough to spread over several data blocks. */
    private static List<Entry> severalBlocks(int count, int valueLength) {
        List<Entry> entries = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            byte[] value = new byte[valueLength];
            Arrays.fill(value, (byte) ('a' + i % 26));
            entries.add(new Entry(
                    InternalKey.encode(ascii(String.format("key-%04d", i)), 100 + i, InternalKey.TYPE_VALUE),
                    value));
        }
        return entries;
    }

    // ---- independent format helpers for the byte-exact test ---------------------------------

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

    private static byte[] le(long value, int width) {
        byte[] out = new byte[width];
        for (int i = 0; i < width; i++) {
            out[i] = (byte) (value >>> (8 * i));
        }
        return out;
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }

    /** Block contents followed by the trailer: type 0 and the context checksum. */
    private static byte[] framed(byte[] contents, long fileNumber, long offset) {
        byte[] type = {0};
        return concat(contents, type,
                le(referenceCrc32c(contents, type, le(fileNumber, 8), le(offset, 8)) & 0xFFFFFFFFL, 4));
    }

    /** Rebuilds the footer of {@code file} with a different magic number and a valid checksum. */
    private static byte[] withMagic(byte[] file, long fileNumber, long magic) {
        byte[] out = file.clone();
        int footer = out.length - 48;
        System.arraycopy(le(magic, 8), 0, out, footer + 40, 8);
        int crc = referenceCrc32c(Arrays.copyOfRange(out, footer, footer + 36), le(magic, 8),
                le(fileNumber, 8), le(footer, 8));
        System.arraycopy(le(crc & 0xFFFFFFFFL, 4), 0, out, footer + 36, 4);
        return out;
    }

    // ---- byte-exact format -----------------------------------------------------------------

    @Test
    void aSmallTableHasExactlyTheSpecifiedBytes() throws IOException {
        Path path = freshPath();
        TableInfo info = writeTable(path, FILE_NUMBER, List.of(put("a", 5, "1"), tombstone("b", 6)));

        byte[] keyA = concat(ascii("a"), new byte[] {-1, -1, -1, -1, -1, -1, -1, (byte) 0xFA, 0x01});
        byte[] keyB = concat(ascii("b"), new byte[] {-1, -1, -1, -1, -1, -1, -1, (byte) 0xF9, 0x00});

        // Data block at offset 0: two entries, one restart point.
        byte[] data = concat(
                new byte[] {0, 10, 1}, keyA, ascii("1"),
                new byte[] {0, 10, 0}, keyB,
                le(0, 4), le(1, 4));
        assertEquals(35, data.length);

        // Filter block at offset 40: one bucket, six probes, bits of the two user keys.
        byte[] filter = new byte[69];
        for (String userKey : new String[] {"a", "b"}) {
            long hash = KeyHash.hash(ascii(userKey));
            int x = (int) hash;
            int delta = (x << 15) | (x >>> 17);
            for (int probe = 0; probe < 6; probe++) {
                int bit = x & 511;
                filter[bit / 8] |= (byte) (1 << (bit % 8));
                x += delta;
            }
        }
        filter[64] = 1;
        filter[68] = 6;

        // Index block at offset 114: one entry that maps the last key to the data block.
        byte[] index = concat(
                new byte[] {0, 10, 16}, keyB, le(0, 8), le(35, 8),
                le(0, 4), le(1, 4));
        assertEquals(37, index.length);

        // Footer at offset 156.
        long magic = 0x4C534D4A32350001L;
        byte[] footerPrefix = concat(le(40, 8), le(69, 8), le(114, 8), le(37, 8), le(0, 4));
        byte[] footer = concat(footerPrefix,
                le(referenceCrc32c(footerPrefix, le(magic, 8), le(FILE_NUMBER, 8), le(156, 8)) & 0xFFFFFFFFL, 4),
                le(magic, 8));
        assertEquals(48, footer.length);

        byte[] expected = concat(
                framed(data, FILE_NUMBER, 0),
                framed(filter, FILE_NUMBER, 40),
                framed(index, FILE_NUMBER, 114),
                footer);
        assertEquals(204, expected.length);
        assertArrayEquals(expected, readAll(path));

        assertEquals(204, info.fileSize());
        assertEquals(FILE_NUMBER, info.fileNumber());
        assertEquals(2, info.entryCount());
        assertArrayEquals(keyA, info.smallestKey());
        assertArrayEquals(keyB, info.largestKey());
        assertEquals(5, info.smallestSequence());
        assertEquals(6, info.largestSequence());
    }

    // ---- reading ---------------------------------------------------------------------------

    @Test
    void entriesAreReadBackInOrderAcrossSeveralBlocks() throws IOException {
        List<Entry> entries = severalBlocks(200, 300);
        Path path = freshPath();
        TableInfo info = writeTable(path, FILE_NUMBER, entries);

        SSTableReader reader = SSTableReader.open(fs, path, FILE_NUMBER);
        try {
            assertEquals(info.fileSize(), reader.fileSize());
            assertEquals(1, reader.formatVersion());
            assertTrue(reader.dataBlocks().size() > 10, "expected several data blocks");
            assertSameEntries(entries, scan(reader));
            for (Entry entry : entries) {
                assertTrue(reader.mayContain(InternalKey.userKey(entry.key())));
            }
        } finally {
            reader.release();
        }
    }

    @Test
    void lookupsHonorTheSnapshot() throws IOException {
        // Internal-key order: user key ascending, newest version first.
        List<Entry> entries = List.of(
                put("apple", 9, "v9"),
                tombstone("apple", 6),
                put("apple", 3, "v3"),
                put("cherry", 4, "c4"));
        Path path = freshPath();
        writeTable(path, FILE_NUMBER, entries);
        SSTableReader reader = SSTableReader.open(fs, path, FILE_NUMBER);
        try {
            assertValue(reader.get(ascii("apple"), Long.MAX_VALUE), "v9", 9);
            assertValue(reader.get(ascii("apple"), 9), "v9", 9);
            assertEquals(Lookup.Kind.TOMBSTONE, reader.get(ascii("apple"), 8).kind());
            assertEquals(6, reader.get(ascii("apple"), 6).sequence());
            assertValue(reader.get(ascii("apple"), 5), "v3", 3);
            assertValue(reader.get(ascii("apple"), 3), "v3", 3);
            assertEquals(Lookup.Kind.ABSENT, reader.get(ascii("apple"), 2).kind());
            assertEquals(Lookup.Kind.ABSENT, reader.get(ascii("banana"), Long.MAX_VALUE).kind());
            assertEquals(Lookup.Kind.ABSENT, reader.get(ascii("cherry"), 3).kind());
            assertValue(reader.get(ascii("cherry"), 4), "c4", 4);
            assertEquals(Lookup.Kind.ABSENT, reader.get(ascii("zebra"), Long.MAX_VALUE).kind());
            assertNull(reader.get(ascii("zebra"), 1).value());
        } finally {
            reader.release();
        }
    }

    private static void assertValue(Lookup lookup, String value, long sequence) {
        assertEquals(Lookup.Kind.VALUE, lookup.kind());
        assertArrayEquals(ascii(value), lookup.value());
        assertEquals(sequence, lookup.sequence());
    }

    @Test
    void randomTablesMatchASortedMapModel() {
        Seeds.forEach(SSTableTest.class, "sstable-model", 60, seed -> {
            Random random = new Random(seed);
            TreeMap<byte[], byte[]> model = new TreeMap<>(InternalKey.COMPARATOR);
            List<byte[]> userKeys = new ArrayList<>();
            int userKeyCount = 1 + random.nextInt(random.nextBoolean() ? 20 : 1500);
            long sequence = 1;
            for (int i = 0; i < userKeyCount; i++) {
                byte[] userKey = new byte[1 + random.nextInt(random.nextInt(10) == 0 ? 200 : 16)];
                for (int j = 0; j < userKey.length; j++) {
                    userKey[j] = (byte) (random.nextInt(3) == 0 ? random.nextInt(256) : random.nextInt(4));
                }
                userKeys.add(userKey);
                int versions = 1 + random.nextInt(4);
                for (int v = 0; v < versions; v++) {
                    boolean isTombstone = random.nextInt(5) == 0;
                    int valueLength = random.nextInt(50) == 0 ? random.nextInt(9000) : random.nextInt(40);
                    byte[] value = new byte[isTombstone ? 0 : valueLength];
                    random.nextBytes(value);
                    model.put(InternalKey.encode(userKey, sequence++,
                            isTombstone ? InternalKey.TYPE_TOMBSTONE : InternalKey.TYPE_VALUE), value);
                }
            }
            long lastSequence = sequence - 1;

            Path path = freshPath();
            List<Entry> entries = new ArrayList<>();
            for (Map.Entry<byte[], byte[]> e : model.entrySet()) {
                entries.add(new Entry(e.getKey(), e.getValue()));
            }
            TableInfo info = writeTable(path, seed, entries);
            assertEquals(fs.size(path), info.fileSize());
            assertEquals(model.size(), info.entryCount());
            assertArrayEquals(model.firstKey(), info.smallestKey());
            assertArrayEquals(model.lastKey(), info.largestKey());

            SSTableReader reader = SSTableReader.open(fs, path, seed);
            try {
                assertSameEntries(entries, scan(reader));

                try (TableIterator it = reader.iterator()) {
                    for (int i = 0; i < 300; i++) {
                        byte[] userKey = userKeys.get(random.nextInt(userKeys.size())).clone();
                        if (random.nextInt(3) == 0) {
                            userKey[random.nextInt(userKey.length)] ^= (byte) (1 << random.nextInt(8));
                        }
                        long snapshot = random.nextLong(lastSequence + 2);

                        // seek against the model
                        byte[] target = InternalKey.seekKey(userKey, snapshot);
                        Map.Entry<byte[], byte[]> expected = model.ceilingEntry(target);
                        it.seek(target);
                        if (expected == null) {
                            assertFalse(it.valid());
                        } else {
                            assertTrue(it.valid());
                            assertArrayEquals(expected.getKey(), it.key());
                            assertArrayEquals(expected.getValue(), it.value());
                        }

                        // point lookup against the model
                        Lookup lookup = reader.get(userKey, snapshot);
                        if (expected == null || !InternalKey.hasUserKey(expected.getKey(), userKey)) {
                            assertEquals(Lookup.Kind.ABSENT, lookup.kind());
                        } else if (InternalKey.type(expected.getKey()) == InternalKey.TYPE_TOMBSTONE) {
                            assertEquals(Lookup.Kind.TOMBSTONE, lookup.kind());
                            assertEquals(InternalKey.sequence(expected.getKey()), lookup.sequence());
                        } else {
                            assertEquals(Lookup.Kind.VALUE, lookup.kind());
                            assertArrayEquals(expected.getValue(), lookup.value());
                            assertEquals(InternalKey.sequence(expected.getKey()), lookup.sequence());
                        }
                    }
                }
                for (byte[] userKey : userKeys) {
                    assertTrue(reader.mayContain(userKey));
                }
            } finally {
                reader.release();
            }
            assertEquals(0, fs.openHandleCount());
            fs.delete(path);
        });
    }

    @Test
    void theFilterRejectsMostAbsentKeys() throws IOException {
        List<Entry> entries = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            entries.add(put(String.format("present-%06d", i), i + 1, "v"));
        }
        Path path = freshPath();
        writeTable(path, FILE_NUMBER, entries);
        SSTableReader reader = SSTableReader.open(fs, path, FILE_NUMBER);
        try {
            int falsePositives = 0;
            for (int i = 0; i < 20_000; i++) {
                if (reader.mayContain(ascii("absent-" + i))) {
                    falsePositives++;
                }
            }
            assertTrue(falsePositives < 600, "false positives: " + falsePositives);
            assertEquals(BloomFilter.bucketCountFor(5000), reader.filterBucketCount());
        } finally {
            reader.release();
        }
    }

    @Test
    void severalVersionsOfOneKeyAreHashedOnce() throws IOException {
        List<Entry> entries = new ArrayList<>();
        for (int version = 2000; version >= 1; version--) {
            entries.add(put("same", version, "v"));
        }
        Path path = freshPath();
        writeTable(path, FILE_NUMBER, entries);
        SSTableReader reader = SSTableReader.open(fs, path, FILE_NUMBER);
        try {
            assertEquals(1, reader.filterBucketCount());
            assertSameEntries(entries, scan(reader));
        } finally {
            reader.release();
        }
    }

    // ---- damage ----------------------------------------------------------------------------

    /** Opens the table and reads everything; returns normally only if nothing was detected. */
    private void readEverything(Path path, long fileNumber) throws IOException {
        SSTableReader reader = SSTableReader.open(fs, path, fileNumber);
        try {
            scan(reader);
        } finally {
            reader.release();
        }
    }

    @Test
    void everySingleBitFlipIsDetected() throws IOException {
        Path original = freshPath();
        writeTable(original, FILE_NUMBER, severalBlocks(18, 700));
        byte[] bytes = readAll(original);
        assertTrue(bytes.length > 3 * 4096);

        Path damagedPath = freshPath();
        for (int bit = 0; bit < bytes.length * 8; bit++) {
            byte[] damaged = bytes.clone();
            damaged[bit >>> 3] ^= (byte) (1 << (bit & 7));
            try (WritableFile file = fs.create(damagedPath)) {
                file.append(damaged, 0, damaged.length);
            }
            try {
                readEverything(damagedPath, FILE_NUMBER);
                fail("bit " + bit + " (byte " + (bit >>> 3) + ") was flipped without detection");
            } catch (CorruptionException expected) {
                // Detected.
            }
            assertEquals(0, fs.openHandleCount(), "handle leaked after damage at bit " + bit);
            fs.delete(damagedPath);
        }
    }

    @Test
    void everyTruncationIsDetected() throws IOException {
        Path original = freshPath();
        writeTable(original, FILE_NUMBER, severalBlocks(6, 500));
        byte[] bytes = readAll(original);
        for (int length = 0; length < bytes.length; length++) {
            Path cut = writeBytes(Arrays.copyOf(bytes, length));
            assertThrows(CorruptionException.class, () -> readEverything(cut, FILE_NUMBER),
                    "length " + length);
            fs.delete(cut);
        }
    }

    @Test
    void aTableOpenedUnderAnotherFileNumberIsRejected() throws IOException {
        Path path = freshPath();
        writeTable(path, FILE_NUMBER, severalBlocks(5, 100));
        readEverything(path, FILE_NUMBER);
        assertThrows(CorruptionException.class, () -> SSTableReader.open(fs, path, FILE_NUMBER + 1));
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void blocksThatSwappedPlacesAreDetected() throws IOException {
        // Entries of identical size give data blocks of identical size.
        Path path = freshPath();
        writeTable(path, FILE_NUMBER, severalBlocks(40, 1000));
        byte[] bytes = readAll(path);

        SSTableReader reader = SSTableReader.open(fs, path, FILE_NUMBER);
        BlockHandle first;
        BlockHandle second;
        try {
            first = reader.dataBlocks().get(0).handle();
            second = reader.dataBlocks().get(1).handle();
        } finally {
            reader.release();
        }
        assertEquals(first.size(), second.size());
        int length = (int) first.size() + TableFormat.BLOCK_TRAILER_LENGTH;
        byte[] swapped = bytes.clone();
        System.arraycopy(bytes, (int) second.offset(), swapped, (int) first.offset(), length);
        System.arraycopy(bytes, (int) first.offset(), swapped, (int) second.offset(), length);

        Path swappedPath = writeBytes(swapped);
        CorruptionException e = assertThrows(CorruptionException.class,
                () -> readEverything(swappedPath, FILE_NUMBER));
        assertEquals(first.offset(), e.offset());
        assertTrue(e.getMessage().contains(swappedPath.toString()));
    }

    @Test
    void aNewerFormatVersionIsRejectedWithoutGuessing() throws IOException {
        Path path = freshPath();
        writeTable(path, FILE_NUMBER, severalBlocks(3, 10));
        byte[] bytes = readAll(path);

        Path newer = writeBytes(withMagic(bytes, FILE_NUMBER, 0x4C534D4A32350002L));
        UnsupportedFormatException e = assertThrows(UnsupportedFormatException.class,
                () -> SSTableReader.open(fs, newer, FILE_NUMBER));
        assertTrue(e.getMessage().contains("version 2"));

        Path versionZero = writeBytes(withMagic(bytes, FILE_NUMBER, 0x4C534D4A32350000L));
        assertThrows(UnsupportedFormatException.class, () -> SSTableReader.open(fs, versionZero, FILE_NUMBER));

        // A different magic number is not a table at all, even with a valid checksum.
        Path foreign = writeBytes(withMagic(bytes, FILE_NUMBER, 0x1122334455660001L));
        assertThrows(CorruptionException.class, () -> SSTableReader.open(fs, foreign, FILE_NUMBER));
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void anUnknownCompressionTypeIsRejectedWithoutGuessing() throws IOException {
        Path path = freshPath();
        writeTable(path, FILE_NUMBER, List.of(put("a", 1, "x")));
        byte[] bytes = readAll(path);
        // The first data block starts at offset 0; rewrite its trailer with type 1 and a
        // checksum that is valid for it.
        SSTableReader reader = SSTableReader.open(fs, path, FILE_NUMBER);
        int size = (int) reader.dataBlocks().get(0).handle().size();
        reader.release();
        byte[] type = {1};
        int crc = referenceCrc32c(Arrays.copyOf(bytes, size), type, le(FILE_NUMBER, 8), le(0, 8));
        bytes[size] = 1;
        System.arraycopy(le(crc & 0xFFFFFFFFL, 4), 0, bytes, size + 1, 4);

        Path compressed = writeBytes(bytes);
        assertThrows(UnsupportedFormatException.class, () -> readEverything(compressed, FILE_NUMBER));
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void aFileThatIsTooSmallIsRejected() throws IOException {
        for (int length : new int[] {0, 1, 47}) {
            Path path = writeBytes(new byte[length]);
            assertThrows(CorruptionException.class, () -> SSTableReader.open(fs, path, FILE_NUMBER));
        }
        Path zeros = writeBytes(new byte[48]);
        assertThrows(CorruptionException.class, () -> SSTableReader.open(fs, zeros, FILE_NUMBER));
    }

    // ---- writer rules ----------------------------------------------------------------------

    @Test
    void theWriterRejectsKeysOutOfOrder() throws IOException {
        try (SSTableWriter writer = new SSTableWriter(fs, freshPath(), FILE_NUMBER)) {
            writer.add(put("b", 5, "x").key(), ascii("x"));
            // Same key again, an older key first, and a smaller user key.
            assertThrows(IllegalArgumentException.class, () -> writer.add(put("b", 5, "x").key(), ascii("x")));
            assertThrows(IllegalArgumentException.class, () -> writer.add(put("b", 6, "x").key(), ascii("x")));
            assertThrows(IllegalArgumentException.class, () -> writer.add(put("a", 1, "x").key(), ascii("x")));
            writer.add(put("b", 4, "x").key(), ascii("x"));
            assertEquals(2, writer.entryCount());
        }
    }

    @Test
    void theWriterRejectsMalformedKeysAndEmptyTables() throws IOException {
        try (SSTableWriter writer = new SSTableWriter(fs, freshPath(), FILE_NUMBER)) {
            assertThrows(IllegalArgumentException.class, () -> writer.add(new byte[5], new byte[0]));
            // An empty user key is not allowed.
            assertThrows(IllegalArgumentException.class,
                    () -> writer.add(InternalKey.encode(new byte[0], 1, InternalKey.TYPE_VALUE), new byte[0]));
            assertThrows(IllegalStateException.class, writer::finish);
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void aFinishedWriterAcceptsNothingMore() throws IOException {
        SSTableWriter writer = new SSTableWriter(fs, freshPath(), FILE_NUMBER);
        writer.add(put("a", 1, "x").key(), ascii("x"));
        writer.finish();
        assertThrows(IllegalStateException.class, () -> writer.add(put("b", 2, "x").key(), ascii("x")));
        assertThrows(IllegalStateException.class, writer::finish);
        writer.close();
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void finishSyncsTheFileContents() throws IOException {
        Path path = freshPath();
        TableInfo info = writeTable(path, FILE_NUMBER, severalBlocks(10, 100));
        fs.syncDirectory(dir);
        fs.crash();
        assertEquals(info.fileSize(), fs.size(path));
        readEverything(path, FILE_NUMBER);
    }

    // ---- reference counting ----------------------------------------------------------------

    @Test
    void theFileStaysOpenUntilTheLastReferenceIsReleased() throws IOException {
        Path path = freshPath();
        writeTable(path, FILE_NUMBER, severalBlocks(10, 100));
        SSTableReader reader = SSTableReader.open(fs, path, FILE_NUMBER);
        assertEquals(1, reader.referenceCount());

        TableIterator it = reader.iterator();
        assertEquals(2, reader.referenceCount());
        reader.release(); // the owner lets go while an iterator is still active
        assertEquals(1, fs.openHandleCount());
        assertThrows(FileSystemException.class, () -> fs.delete(path));

        it.seekToFirst();
        assertTrue(it.valid());
        it.close();
        it.close();
        assertEquals(0, reader.referenceCount());
        assertEquals(0, fs.openHandleCount());
        fs.delete(path);

        assertThrows(IllegalStateException.class, reader::retain);
        assertThrows(IllegalStateException.class, reader::iterator);
        assertThrows(IllegalStateException.class, reader::release);
        assertThrows(IllegalStateException.class, it::seekToFirst);
    }

    @Test
    void userKeysAreComparedAsUnsignedBytes() throws IOException {
        byte[] low = {0x7F};
        byte[] high = {(byte) 0x80};
        assertTrue(ByteComparator.compare(low, high) < 0);
        List<Entry> entries = List.of(
                new Entry(InternalKey.encode(low, 1, InternalKey.TYPE_VALUE), ascii("low")),
                new Entry(InternalKey.encode(high, 2, InternalKey.TYPE_VALUE), ascii("high")));
        Path path = freshPath();
        writeTable(path, FILE_NUMBER, entries);
        SSTableReader reader = SSTableReader.open(fs, path, FILE_NUMBER);
        try {
            assertSameEntries(entries, scan(reader));
            assertArrayEquals(ascii("high"), reader.get(high, Long.MAX_VALUE).value());
        } finally {
            reader.release();
        }
    }
}
