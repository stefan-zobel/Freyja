package io.github.stefan_zobel.freyja.lsm.log;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.common.testing.Seeds;
import io.github.stefan_zobel.freyja.lsm.CorruptionException;
import io.github.stefan_zobel.freyja.lsm.UnsupportedFormatException;
import io.github.stefan_zobel.freyja.lsm.fs.MemoryFileSystem;
import io.github.stefan_zobel.freyja.lsm.fs.ReadableFile;
import io.github.stefan_zobel.freyja.lsm.fs.WritableFile;

/**
 * Tests of the WAL record format: fragmenting (docs/spec/verification.md, section 2.8),
 * sync points, and the recovery classification (docs/spec/storage-format.md, section 3.2).
 */
class LogTest {

    private static final int BLOCK = 32768;
    private static final int HEADER = 7;
    private static final long FILE_NUMBER = 11;

    /** Offset after the header record: one fragment with a 7-byte payload. */
    private static final int AFTER_FILE_HEADER = 14;

    private final MemoryFileSystem fs = new MemoryFileSystem();
    private final Path dir = Path.of("log-test").toAbsolutePath();
    private int nextName;

    @BeforeEach
    void createDirectory() throws IOException {
        fs.createDirectories(dir);
    }

    // ---- helpers ---------------------------------------------------------------------------

    private Path freshPath() {
        return dir.resolve("log-" + (nextName++));
    }

    /**
     * Deletes a file for good. The file system keeps a deleted file until the removal of its
     * name is durable, because a crash before that would bring it back; a test that writes
     * many files would hold all of them in memory.
     */
    private void discard(Path path) throws IOException {
        fs.delete(path);
        fs.syncDirectory(dir);
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
        }
        return path;
    }

    private static byte[] payload(int length, int salt) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (i * 31 + salt);
        }
        return bytes;
    }

    /** Everything a replay reported. */
    private static final class Replay implements LogReader.Listener {
        final List<byte[]> records = new ArrayList<>();
        final List<Long> recordOffsets = new ArrayList<>();
        final List<long[]> syncPoints = new ArrayList<>();
        LogReader.Result result;

        @Override
        public void record(long offset, byte[] payload) {
            recordOffsets.add(offset);
            records.add(payload);
        }

        @Override
        public void syncPoint(long offset, long syncedLength) {
            syncPoints.add(new long[] {offset, syncedLength});
        }
    }

    private Replay replay(Path path, long fileNumber, FileKind kind, boolean sealed) throws IOException {
        Replay replay = new Replay();
        try (ReadableFile file = fs.open(path)) {
            replay.result = LogReader.read(file, path.toString(), fileNumber, kind, sealed, replay);
        }
        return replay;
    }

    private Replay replay(Path path, boolean sealed) throws IOException {
        return replay(path, FILE_NUMBER, FileKind.WAL, sealed);
    }

    /** One fragment as found by the structural walk. */
    private record Fragment(long offset, int type, int length) {
    }

    /**
     * Walks the raw bytes by structure alone, independently of the reader, and checks the
     * layout rules: no header and no payload crosses a block boundary, padding is zero, and
     * fragment types form valid sequences.
     */
    private static List<Fragment> walk(byte[] bytes) {
        List<Fragment> fragments = new ArrayList<>();
        boolean inRecord = false;
        int pos = 0;
        while (pos < bytes.length) {
            int room = BLOCK - pos % BLOCK;
            if (room < HEADER) {
                for (int i = pos; i < Math.min(pos + room, bytes.length); i++) {
                    assertEquals(0, bytes[i], "padding at " + i);
                }
                pos += room;
                continue;
            }
            assertTrue(bytes.length - pos >= HEADER, "header cut off at " + pos);
            int length = (bytes[pos + 4] & 0xFF) | (bytes[pos + 5] & 0xFF) << 8;
            int type = bytes[pos + 6] & 0xFF;
            assertTrue(type >= 1 && type <= 5, "type " + type + " at " + pos);
            assertTrue(length <= room - HEADER, "fragment at " + pos + " crosses a block boundary");
            assertTrue(pos + HEADER + length <= bytes.length, "fragment at " + pos + " cut off");
            if (type == 5) {
                assertEquals(8, length, "sync point length at " + pos);
            } else {
                boolean starts = (type == 1 || type == 2);
                assertEquals(!inRecord, starts, "fragment sequence at " + pos);
                inRecord = (type == 2 || type == 3);
            }
            fragments.add(new Fragment(pos, type, length));
            pos += HEADER + length;
        }
        assertFalse(inRecord, "file ends inside a record");
        return fragments;
    }

    /** Concatenates the payloads of the last logical record found by the structural walk. */
    private static byte[] lastRecord(byte[] bytes, List<Fragment> fragments) {
        int end = fragments.size() - 1;
        while (fragments.get(end).type() == 5) {
            end--;
        }
        int start = end;
        while (fragments.get(start).type() != 1 && fragments.get(start).type() != 2) {
            start--;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = start; i <= end; i++) {
            Fragment f = fragments.get(i);
            if (f.type() != 5) {
                out.write(bytes, (int) f.offset() + HEADER, f.length());
            }
        }
        return out.toByteArray();
    }

    // ---- independent format helpers --------------------------------------------------------

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

    /** A fragment assembled by hand from the format specification. */
    private static byte[] fragment(int type, byte[] payload, long fileNumber, long offset) {
        byte[] covered = concat(le(payload.length, 2), new byte[] {(byte) type}, payload);
        int crc = referenceCrc32c(covered, le(fileNumber, 8), le(offset, 8));
        return concat(le(crc & 0xFFFFFFFFL, 4), covered);
    }

    private static byte[] fileHeader(int kind, int version, long fileNumber) {
        return fragment(1, concat(new byte[] {'F', 'R', 'Y', 'J', (byte) kind}, le(version, 2)), fileNumber, 0);
    }

    // ---- byte-exact format -----------------------------------------------------------------

    @Test
    void aSmallLogHasExactlyTheSpecifiedBytes() throws IOException {
        Path path = freshPath();
        try (LogWriter writer = new LogWriter(fs.create(path), FILE_NUMBER, FileKind.WAL)) {
            writer.addRecord(new byte[] {1, 2, 3});
            writer.sync();
            writer.addRecord(new byte[0]);
            writer.sync();
        }
        byte[] expected = concat(
                fileHeader(1, 1, FILE_NUMBER),                               // offset 0, 14 bytes
                fragment(1, new byte[] {1, 2, 3}, FILE_NUMBER, 14),          // offset 14, 10 bytes
                fragment(5, le(24, 8), FILE_NUMBER, 24),                     // sync point, 15 bytes
                fragment(1, new byte[0], FILE_NUMBER, 39));                  // empty record
        assertArrayEquals(expected, readAll(path));

        Replay replay = replay(path, true);
        assertEquals(FileKind.WAL, replay.result.kind());
        assertFalse(replay.result.tornTail());
        assertEquals(46, replay.result.validLength());
        assertEquals(2, replay.records.size());
        assertArrayEquals(new byte[] {1, 2, 3}, replay.records.get(0));
        assertEquals(0, replay.records.get(1).length);
        assertEquals(List.of(14L, 39L), replay.recordOffsets);
        assertEquals(1, replay.syncPoints.size());
        assertArrayEquals(new long[] {24, 24}, replay.syncPoints.get(0));
    }

    // ---- fragmenting, exhaustive -----------------------------------------------------------

    /** Brings a new writer to the in-block offset {@code start} and returns the writer. */
    private LogWriter writerAt(Path path, int start) throws IOException {
        LogWriter writer = new LogWriter(fs.create(path), FILE_NUMBER, FileKind.WAL);
        if (start >= AFTER_FILE_HEADER + HEADER) {
            writer.addRecord(new byte[start - AFTER_FILE_HEADER - HEADER]);
        } else {
            // Fill the first block exactly, then position inside the second one.
            writer.addRecord(new byte[BLOCK - AFTER_FILE_HEADER - HEADER]);
            if (start > 0) {
                writer.addRecord(new byte[start - HEADER]);
            }
        }
        assertEquals(start, writer.size() % BLOCK);
        return writer;
    }

    /** Capacity of the first fragment of a record written at in-block offset {@code start}. */
    private static int firstFragmentCapacity(int start, boolean syncPointPending) {
        int room = BLOCK - start;
        if (room < HEADER) {
            room = BLOCK;
        }
        if (syncPointPending && room >= HEADER + 8) {
            room -= HEADER + 8;
            if (room < HEADER) {
                room = BLOCK;
            }
        }
        return room - HEADER;
    }

    private void checkFragmenting(int start, int length, boolean withSyncPoint) throws IOException {
        Path path = freshPath();
        byte[] record = payload(length, start);
        long syncedLength;
        try (LogWriter writer = writerAt(path, start)) {
            syncedLength = writer.size();
            if (withSyncPoint) {
                writer.sync();
            }
            writer.addRecord(record);
        }
        byte[] bytes = readAll(path);
        String what = "start " + start + ", length " + length + ", sync point " + withSyncPoint;

        // Structure, checked without the reader.
        List<Fragment> fragments = walk(bytes);
        if (!Arrays.equals(record, lastRecord(bytes, fragments))) {
            fail("raw fragments do not hold the payload: " + what);
        }
        // A sync point needs 15 bytes. If fewer remain in the block and the record ends in
        // this block, the sync point stays pending for the next record.
        int room = BLOCK - start;
        boolean deferred = room >= HEADER && room < HEADER + 8 && length <= room - HEADER;
        boolean expectSyncPoint = withSyncPoint && !deferred;
        long syncPoints = fragments.stream().filter(f -> f.type() == 5).count();
        assertEquals(expectSyncPoint ? 1 : 0, syncPoints, what);

        // Round trip through the reader.
        Replay replay = replay(path, true);
        if (!Arrays.equals(record, replay.records.get(replay.records.size() - 1))) {
            fail("round trip failed: " + what);
        }
        if (expectSyncPoint) {
            assertEquals(syncedLength, replay.syncPoints.get(0)[1], what);
        }
        discard(path);
    }

    /** In-block offsets a writer can be at: 0, and 7 or more bytes into a block. */
    private static boolean reachable(int start) {
        return start == 0 || start >= HEADER;
    }

    @Test
    void recordsRoundTripFromEveryStartOffsetAroundTheFirstFragmentBoundary() throws IOException {
        for (int start = 0; start < BLOCK; start++) {
            if (!reachable(start)) {
                continue;
            }
            int capacity = firstFragmentCapacity(start, false);
            for (int length : new int[] {capacity - 1, capacity, capacity + 1}) {
                if (length >= 0) {
                    checkFragmenting(start, length, false);
                }
            }
        }
    }

    @Test
    void recordsWithAPendingSyncPointRoundTripFromEveryStartOffset() throws IOException {
        for (int start = 0; start < BLOCK; start++) {
            if (!reachable(start)) {
                continue;
            }
            int capacity = firstFragmentCapacity(start, true);
            checkFragmenting(start, capacity, true);
            // Near the end of a block the sync point changes where fragments fall.
            if (BLOCK - start <= 64 || start <= 64) {
                for (int length : new int[] {0, 1, capacity - 1, capacity + 1, capacity + BLOCK - HEADER}) {
                    if (length >= 0) {
                        checkFragmenting(start, length, true);
                    }
                }
            }
        }
    }

    @Test
    void recordsRoundTripAroundEveryLaterFragmentBoundary() throws IOException {
        int perBlock = BLOCK - HEADER;
        for (int start = 0; start < BLOCK; start++) {
            boolean selected = start <= 48 || BLOCK - start <= 48 || start % 1021 == 0;
            if (!reachable(start) || !selected) {
                continue;
            }
            for (boolean sync : new boolean[] {false, true}) {
                int capacity = firstFragmentCapacity(start, sync);
                for (int blocks = 1; blocks <= 2; blocks++) {
                    for (int delta = -1; delta <= 1; delta++) {
                        checkFragmenting(start, capacity + blocks * perBlock + delta, sync);
                    }
                }
                checkFragmenting(start, 0, sync);
                checkFragmenting(start, 1, sync);
            }
        }
    }

    @Test
    void aSyncPointThatDoesNotFitMovesToTheNextBlock() throws IOException {
        // 14 bytes remain: enough for a fragment header, not for a sync point.
        Path path = freshPath();
        try (LogWriter writer = writerAt(path, BLOCK - 14)) {
            writer.sync();
            writer.addRecord(payload(100, 1));
        }
        List<Fragment> fragments = walk(readAll(path));
        Fragment first = fragments.get(fragments.size() - 3);
        Fragment syncPoint = fragments.get(fragments.size() - 2);
        Fragment last = fragments.get(fragments.size() - 1);
        assertEquals(new Fragment(BLOCK - 14, 2, 7), first);
        assertEquals(new Fragment(BLOCK, 5, 8), syncPoint);
        assertEquals(new Fragment(BLOCK + 15, 4, 93), last);

        Replay replay = replay(path, true);
        assertArrayEquals(payload(100, 1), replay.records.get(replay.records.size() - 1));
        assertArrayEquals(new long[] {BLOCK, BLOCK - 14}, replay.syncPoints.get(0));
    }

    @Test
    void exactlySevenRemainingBytesHoldAnEmptyFirstFragment() throws IOException {
        Path path = freshPath();
        try (LogWriter writer = writerAt(path, BLOCK - 7)) {
            writer.addRecord(payload(5, 2));
        }
        List<Fragment> fragments = walk(readAll(path));
        assertEquals(new Fragment(BLOCK - 7, 2, 0), fragments.get(fragments.size() - 2));
        assertEquals(new Fragment(BLOCK, 4, 5), fragments.get(fragments.size() - 1));
    }

    // ---- randomized round trip -------------------------------------------------------------

    @Test
    void randomLogsRoundTrip() {
        Seeds.forEach(LogTest.class, "log-round-trip", 150, seed -> {
            Random random = new Random(seed);
            Path path = freshPath();
            List<byte[]> written = new ArrayList<>();
            List<Long> syncedLengths = new ArrayList<>();
            try (LogWriter writer = new LogWriter(fs.create(path), seed, FileKind.MANIFEST)) {
                int count = random.nextInt(60);
                for (int i = 0; i < count; i++) {
                    int length = switch (random.nextInt(6)) {
                        case 0 -> 0;
                        case 1 -> random.nextInt(100_000);
                        case 2 -> BLOCK - HEADER - random.nextInt(40);
                        default -> random.nextInt(300);
                    };
                    byte[] record = new byte[length];
                    random.nextBytes(record);
                    writer.addRecord(record);
                    written.add(record);
                    if (random.nextInt(3) == 0) {
                        writer.sync();
                        if (syncedLengths.isEmpty() || syncedLengths.get(syncedLengths.size() - 1) != writer.size()) {
                            syncedLengths.add(writer.size());
                        }
                    }
                }
                assertEquals(fs.size(path), writer.size());
            }
            walk(readAll(path));

            Replay replay = replay(path, seed, FileKind.MANIFEST, random.nextBoolean());
            assertFalse(replay.result.tornTail());
            assertEquals(fs.size(path), replay.result.validLength());
            assertEquals(written.size(), replay.records.size());
            for (int i = 0; i < written.size(); i++) {
                assertArrayEquals(written.get(i), replay.records.get(i), "record " + i);
            }
            // Every sync is reported, in order, except a last one that nothing followed.
            assertTrue(replay.syncPoints.size() == syncedLengths.size()
                    || replay.syncPoints.size() == syncedLengths.size() - 1);
            for (int i = 0; i < replay.syncPoints.size(); i++) {
                assertEquals(syncedLengths.get(i), replay.syncPoints.get(i)[1]);
            }
            discard(path);
        });
    }

    // ---- recovery classification -----------------------------------------------------------

    /**
     * A log with three sync rounds. Layout (offsets in the returned array):
     * header, r1, r2 | sync | sp1, r3, r4 | sync | sp2, r5 | sync | sp3, r6 (never synced).
     */
    private record Rounds(Path path, byte[] bytes, long synced1, long synced2, long synced3,
                          List<byte[]> records) {
    }

    private Rounds threeRounds() throws IOException {
        Path path = freshPath();
        List<byte[]> records = List.of(payload(40, 1), payload(70_000, 2), payload(10, 3),
                payload(500, 4), payload(33_000, 5), payload(60, 6));
        long synced1;
        long synced2;
        long synced3;
        try (LogWriter writer = new LogWriter(fs.create(path), FILE_NUMBER, FileKind.WAL)) {
            writer.addRecord(records.get(0));
            writer.addRecord(records.get(1));
            writer.sync();
            synced1 = writer.size();
            writer.addRecord(records.get(2));
            writer.addRecord(records.get(3));
            writer.sync();
            synced2 = writer.size();
            writer.addRecord(records.get(4));
            writer.sync();
            synced3 = writer.size();
            writer.addRecord(records.get(5));
        }
        return new Rounds(path, readAll(path), synced1, synced2, synced3, records);
    }

    private static void assertPrefix(List<byte[]> all, List<byte[]> actual, int atLeast) {
        assertTrue(actual.size() >= atLeast && actual.size() <= all.size(),
                "delivered " + actual.size() + " records, expected at least " + atLeast);
        for (int i = 0; i < actual.size(); i++) {
            assertArrayEquals(all.get(i), actual.get(i), "record " + i);
        }
    }

    @Test
    void aCleanLogIsReportedAsClean() throws IOException {
        Rounds rounds = threeRounds();
        for (boolean sealed : new boolean[] {true, false}) {
            Replay replay = replay(rounds.path(), sealed);
            assertFalse(replay.result.tornTail());
            assertEquals(-1, replay.result.failureOffset());
            assertEquals(rounds.bytes().length, replay.result.validLength());
            assertPrefix(rounds.records(), replay.records, 6);
            assertEquals(3, replay.syncPoints.size());
            assertEquals(rounds.synced3(), replay.syncPoints.get(2)[1]);
        }
    }

    @Test
    void anEmptyFileIsAValidLogWithoutRecords() throws IOException {
        Path path = writeBytes(new byte[0]);
        for (boolean sealed : new boolean[] {true, false}) {
            Replay replay = replay(path, sealed);
            assertFalse(replay.result.tornTail());
            assertNull(replay.result.kind());
            assertEquals(0, replay.result.validLength());
            assertTrue(replay.records.isEmpty());
        }
    }

    @Test
    void everyCutInsideTheUnsyncedTailIsATornTail() throws IOException {
        Rounds rounds = threeRounds();
        byte[] bytes = rounds.bytes();
        for (int length = (int) rounds.synced3(); length < bytes.length; length++) {
            Path cut = writeBytes(Arrays.copyOf(bytes, length));
            Replay replay = replay(cut, false);
            // The tail holds a sync point and the unsynced record 6; cutting it loses at most those.
            assertPrefix(rounds.records(), replay.records, 5);
            assertEquals(5, replay.records.size());
            assertTrue(replay.result.validLength() >= rounds.synced3());
            assertTrue(replay.result.validLength() <= length);
            boolean atBoundary = length == rounds.synced3() || length == rounds.synced3() + 15;
            assertEquals(!atBoundary, replay.result.tornTail(), "length " + length);

            // The same file as a sealed file is damaged unless it ends on a record boundary.
            if (!atBoundary) {
                assertThrows(CorruptionException.class, () -> replay(cut, true));
            }
            // Cutting at the reported length gives a clean log.
            fs.truncate(cut, replay.result.validLength());
            Replay again = replay(cut, true);
            assertFalse(again.result.tornTail());
            assertEquals(5, again.records.size());
            fs.delete(cut);
        }
    }

    @Test
    void aGarbledUnsyncedTailIsATornTail() throws IOException {
        Rounds rounds = threeRounds();
        Random random = new Random(5);
        for (int round = 0; round < 200; round++) {
            byte[] bytes = rounds.bytes().clone();
            int from = (int) rounds.synced3() + random.nextInt(bytes.length - (int) rounds.synced3());
            for (int i = from; i < bytes.length; i++) {
                bytes[i] = (round % 2 == 0) ? 0 : (byte) random.nextInt(256);
            }
            byte[] extra = new byte[random.nextInt(3) == 0 ? random.nextInt(100_000) : 0];
            if (round % 4 == 1) {
                random.nextBytes(extra);
            }
            Path garbled = writeBytes(concat(bytes, extra));
            Replay replay = replay(garbled, false);
            assertPrefix(rounds.records(), replay.records, 5);
            assertTrue(replay.result.validLength() >= rounds.synced3());
            fs.delete(garbled);
        }
    }

    @Test
    void everyBitFlipBelowTheLastProvenSyncedLengthIsDamage() throws IOException {
        Rounds rounds = threeRounds();
        byte[] bytes = rounds.bytes();
        // Every byte below the length recorded by a sync point that survives the flip is
        // proven synced. A flip below the second synced length leaves the third sync point
        // intact, and that one records an even larger length.
        Random random = new Random(9);
        int limit = (int) rounds.synced2();
        for (int i = 0; i < 4000; i++) {
            int bit = (i < 2000) ? random.nextInt(limit * 8) : random.nextInt(4096 * 8);
            byte[] damaged = bytes.clone();
            damaged[bit >>> 3] ^= (byte) (1 << (bit & 7));
            Path path = writeBytes(damaged);
            CorruptionException e = assertThrows(CorruptionException.class, () -> replay(path, false),
                    "bit " + bit);
            assertTrue(e.offset() <= (bit >>> 3), "reported offset " + e.offset() + " for byte " + (bit >>> 3));
            assertThrows(CorruptionException.class, () -> replay(path, true));
            discard(path);
        }
    }

    @Test
    void everyBitFlipInTheFirstBlockIsDamage() throws IOException {
        // Small records and frequent syncs: every header and payload byte of the first
        // records is flipped once.
        Path path = freshPath();
        try (LogWriter writer = new LogWriter(fs.create(path), FILE_NUMBER, FileKind.WAL)) {
            for (int i = 0; i < 40; i++) {
                writer.addRecord(payload(i, i));
                writer.sync();
            }
            writer.addRecord(payload(5, 99));
        }
        byte[] bytes = readAll(path);
        Replay clean = replay(path, false);
        long provenSynced = clean.syncPoints.get(clean.syncPoints.size() - 1)[1];
        long lastSyncPointOffset = clean.syncPoints.get(clean.syncPoints.size() - 1)[0];
        // The second-to-last sync point proves everything below its own synced length even
        // when the last one is hit.
        long safeLimit = clean.syncPoints.get(clean.syncPoints.size() - 2)[1];
        assertTrue(safeLimit < provenSynced && provenSynced <= lastSyncPointOffset);

        for (int bit = 0; bit < safeLimit * 8; bit++) {
            byte[] damaged = bytes.clone();
            damaged[bit >>> 3] ^= (byte) (1 << (bit & 7));
            Path damagedPath = writeBytes(damaged);
            int flipped = bit;
            assertThrows(CorruptionException.class, () -> replay(damagedPath, false), () -> "bit " + flipped);
            fs.delete(damagedPath);
        }
    }

    @Test
    void theStatedResidualCaseIsClassifiedAsATornTail() throws IOException {
        // Damage that hits only data of the final completed sync round, with no later sync
        // point in the file, cannot be told from a torn tail.
        Path path = freshPath();
        long synced1;
        try (LogWriter writer = new LogWriter(fs.create(path), FILE_NUMBER, FileKind.WAL)) {
            writer.addRecord(payload(50, 1));
            writer.sync();
            synced1 = writer.size();
            writer.addRecord(payload(50, 2));
            writer.sync(); // synced, but no later write carries a sync point for it
        }
        byte[] bytes = readAll(path);
        bytes[bytes.length - 1] ^= 0x01;
        Path damaged = writeBytes(bytes);

        Replay replay = replay(damaged, false);
        assertTrue(replay.result.tornTail());
        assertEquals(1, replay.records.size());
        assertEquals(synced1 + 15, replay.result.validLength());
        // As a sealed file the same bytes are damage.
        assertThrows(CorruptionException.class, () -> replay(damaged, true));
    }

    @Test
    void aSyncPointBeyondGarbageProvesDamage() throws IOException {
        Rounds rounds = threeRounds();
        byte[] bytes = rounds.bytes().clone();
        // Destroy everything between the first record and the last sync point, including the
        // two earlier sync points.
        Replay clean = replay(rounds.path(), false);
        int lastSyncPoint = (int) clean.syncPoints.get(2)[0];
        Random random = new Random(3);
        for (int i = 100; i < lastSyncPoint; i++) {
            bytes[i] = (byte) random.nextInt(256);
        }
        Path path = writeBytes(bytes);
        CorruptionException e = assertThrows(CorruptionException.class, () -> replay(path, false));
        assertTrue(e.getMessage().contains("sync point at " + lastSyncPoint), e.getMessage());
    }

    @Test
    void aFileShorterThanAnEarlierSyncPointRecordsIsDamage() throws IOException {
        // A sync point inside a record that never completes: the bytes it proves synced lie
        // beyond the last complete record.
        Path path = freshPath();
        try (LogWriter writer = writerAt(path, BLOCK - 14)) {
            writer.sync();
            writer.addRecord(payload(100, 1));
        }
        byte[] bytes = readAll(path);
        Path complete = writeBytes(bytes);
        assertFalse(replay(complete, false).result.tornTail());

        // Cut inside the last fragment: the record is incomplete, the truncation point is
        // its start, and that equals the synced length. This is a torn tail.
        Path cut = writeBytes(Arrays.copyOf(bytes, bytes.length - 1));
        Replay replay = replay(cut, false);
        assertTrue(replay.result.tornTail());
        assertEquals(BLOCK - 14, replay.result.validLength());

        // A sync point that claims more than the truncation point turns it into damage.
        byte[] forged = Arrays.copyOf(bytes, bytes.length - 1);
        byte[] claim = fragment(5, le(BLOCK - 13, 8), FILE_NUMBER, BLOCK);
        System.arraycopy(claim, 0, forged, BLOCK, claim.length);
        Path forgedPath = writeBytes(forged);
        assertThrows(CorruptionException.class, () -> replay(forgedPath, false));
    }

    @Test
    void aSealedFileWithAnyDefectIsDamaged() throws IOException {
        Rounds rounds = threeRounds();
        byte[] bytes = rounds.bytes();
        // Flip one bit in the unsynced tail and one in the very last byte.
        for (int index : new int[] {(int) rounds.synced3() + 20, bytes.length - 1}) {
            byte[] damaged = bytes.clone();
            damaged[index] ^= 0x40;
            Path path = writeBytes(damaged);
            assertThrows(CorruptionException.class, () -> replay(path, true));
            assertTrue(replay(path, false).result.tornTail());
        }
    }

    @Test
    void aLogUnderAnotherFileNumberOrOfAnotherKindIsRejected() throws IOException {
        Rounds rounds = threeRounds();
        assertThrows(CorruptionException.class,
                () -> replay(rounds.path(), FILE_NUMBER + 1, FileKind.WAL, true));
        CorruptionException e = assertThrows(CorruptionException.class,
                () -> replay(rounds.path(), FILE_NUMBER, FileKind.MANIFEST, true));
        assertTrue(e.getMessage().contains("MANIFEST"), e.getMessage());
        // Without an expected kind the reader reports what it found.
        assertEquals(FileKind.WAL, replay(rounds.path(), FILE_NUMBER, null, true).result.kind());
    }

    @Test
    void unknownVersionsAndKindsAreRejectedWithoutGuessing() throws IOException {
        Path newer = writeBytes(fileHeader(1, 2, FILE_NUMBER));
        UnsupportedFormatException e = assertThrows(UnsupportedFormatException.class, () -> replay(newer, true));
        assertTrue(e.getMessage().contains("version 2"), e.getMessage());

        Path unknownKind = writeBytes(fileHeader(9, 1, FILE_NUMBER));
        assertThrows(UnsupportedFormatException.class, () -> replay(unknownKind, true));

        // A first record that is not a header at all.
        Path noHeader = writeBytes(fragment(1, new byte[] {1, 2, 3}, FILE_NUMBER, 0));
        assertThrows(CorruptionException.class, () -> replay(noHeader, false));

        byte[] wrongMagic = fragment(1, concat(new byte[] {'X', 'R', 'Y', 'J', 1}, le(1, 2)), FILE_NUMBER, 0);
        Path foreign = writeBytes(wrongMagic);
        assertThrows(CorruptionException.class, () -> replay(foreign, false));
    }

    @Test
    void aTornHeaderLeavesAnEmptyLog() throws IOException {
        byte[] header = fileHeader(1, 1, FILE_NUMBER);
        for (int length = 1; length < header.length; length++) {
            Path path = writeBytes(Arrays.copyOf(header, length));
            Replay replay = replay(path, false);
            assertTrue(replay.result.tornTail());
            assertEquals(0, replay.result.validLength());
            assertNull(replay.result.kind());
            assertThrows(CorruptionException.class, () -> replay(path, true));
        }
    }

    @Test
    void aWriterNeedsAnEmptyFile() throws IOException {
        Path path = writeBytes(new byte[] {1});
        discard(path);
        WritableFile file = fs.create(path);
        file.append(new byte[] {1}, 0, 1);
        assertThrows(IllegalArgumentException.class, () -> new LogWriter(file, 1, FileKind.WAL));
        file.close();
    }

    @Test
    void syncingTwiceWithoutNewDataWritesOneSyncPoint() throws IOException {
        Path path = freshPath();
        try (LogWriter writer = new LogWriter(fs.create(path), FILE_NUMBER, FileKind.WAL)) {
            writer.addRecord(payload(10, 1));
            writer.sync();
            writer.sync();
            assertEquals(writer.size(), writer.syncedLength());
            writer.addRecord(payload(10, 2));
            writer.addRecord(payload(10, 3));
        }
        assertEquals(1, replay(path, true).syncPoints.size());
    }
}
