package io.github.stefan_zobel.freyja.lsm;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.common.LittleEndian;
import io.github.stefan_zobel.freyja.lsm.fs.MemoryFileSystem;
import io.github.stefan_zobel.freyja.lsm.fs.ReadableFile;
import io.github.stefan_zobel.freyja.lsm.fs.WritableFile;
import io.github.stefan_zobel.freyja.lsm.log.FileKind;
import io.github.stefan_zobel.freyja.lsm.log.LogWriter;
import io.github.stefan_zobel.freyja.lsm.manifest.CurrentFile;
import io.github.stefan_zobel.freyja.lsm.manifest.FileNames;

/** Behavior of the engine: writes, flushes, reads, recovery, and failure handling. */
class LsmEngineTest {

    private final MemoryFileSystem fs = new MemoryFileSystem();
    private final Path dir = Path.of("engine-test").toAbsolutePath();
    private final EngineOptions options = new EngineOptions().deterministic(true).blockCacheCapacity(1 << 20);

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String s(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    private LsmEngine open() throws IOException {
        return LsmEngine.open(fs, dir, options);
    }

    private byte[] readAll(Path path) throws IOException {
        try (ReadableFile file = fs.open(path)) {
            byte[] bytes = new byte[(int) file.size()];
            file.readFully(0, bytes, 0, bytes.length);
            return bytes;
        }
    }

    private void replaceFile(Path path, byte[] bytes) throws IOException {
        if (fs.exists(path)) {
            fs.delete(path);
        }
        try (WritableFile file = fs.create(path)) {
            file.append(bytes, 0, bytes.length);
            file.sync();
        }
        fs.syncDirectory(dir);
    }

    /** Returns the files of the engine directory whose names end with {@code suffix}. */
    private List<Path> files(String suffix) throws IOException {
        List<Path> result = new ArrayList<>();
        for (Path path : fs.list(dir)) {
            if (path.getFileName().toString().endsWith(suffix)) {
                result.add(path);
            }
        }
        return result;
    }

    private List<String> names() throws IOException {
        List<String> result = new ArrayList<>();
        for (Path path : fs.list(dir)) {
            result.add(path.getFileName().toString());
        }
        return result;
    }

    private Path onlyLog() throws IOException {
        List<Path> logs = files(".wal");
        assertEquals(1, logs.size(), "logs: " + logs);
        return logs.get(0);
    }

    // ---- basic behavior --------------------------------------------------------------------

    @Test
    void valuesCanBeWrittenOverwrittenAndDeleted() throws IOException {
        try (LsmEngine engine = open()) {
            assertNull(engine.get(b("k")));
            engine.put(b("k"), b("v1"));
            assertEquals("v1", s(engine.get(b("k"))));
            engine.put(b("k"), b("v2"));
            assertEquals("v2", s(engine.get(b("k"))));
            engine.delete(b("k"));
            assertNull(engine.get(b("k")));
            engine.put(b("k"), b(""));
            assertArrayEquals(new byte[0], engine.get(b("k")));
            assertEquals(4, engine.lastSequence());
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void aBatchIsAppliedInOrder() throws IOException {
        try (LsmEngine engine = open()) {
            engine.write(new WriteBatch()
                    .put(b("a"), b("1"))
                    .put(b("b"), b("2"))
                    .delete(b("a"))
                    .put(b("c"), b("3"))
                    .put(b("c"), b("4")), true);
            assertNull(engine.get(b("a")));
            assertEquals("2", s(engine.get(b("b"))));
            assertEquals("4", s(engine.get(b("c"))));
            assertEquals(5, engine.lastSequence());

            engine.write(new WriteBatch(), true); // an empty batch does nothing
            assertEquals(5, engine.lastSequence());
        }
    }

    @Test
    void limitsAreEnforced() throws IOException {
        options.maxKeySize(8).maxValueSize(16).maxBatchSize(100);
        try (LsmEngine engine = open()) {
            engine.put(new byte[8], new byte[16]);
            assertThrows(IllegalArgumentException.class, () -> engine.put(new byte[9], new byte[1]));
            assertThrows(IllegalArgumentException.class, () -> engine.put(new byte[1], new byte[17]));
            assertThrows(IllegalArgumentException.class, () -> engine.put(new byte[0], new byte[1]));
            assertThrows(IllegalArgumentException.class, () -> engine.get(new byte[0]));
            WriteBatch large = new WriteBatch();
            for (int i = 0; i < 10; i++) {
                large.put(new byte[] {(byte) i}, new byte[8]);
            }
            assertThrows(IllegalArgumentException.class, () -> engine.write(large, true));
            assertEquals(1, engine.lastSequence());
        }
    }

    // ---- flush and reads from table files --------------------------------------------------

    @Test
    void flushedDataIsReadFromTableFiles() throws IOException {
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("1"));
            engine.put(b("b"), b("2"));
            engine.flush();
            assertEquals(1, engine.tableCount(0));
            assertEquals("1", s(engine.get(b("a"))));
            assertEquals("2", s(engine.get(b("b"))));
            assertNull(engine.get(b("c")));

            engine.flush(); // nothing to flush
            assertEquals(1, engine.tableCount(0));
            assertEquals(1, files(".sst").size());
            assertEquals(1, files(".wal").size(), "the log of the flushed MemTable is gone");
        }
    }

    @Test
    void newerDataShadowsOlderDataAcrossMemTableAndTables() throws IOException {
        try (LsmEngine engine = open()) {
            engine.put(b("k"), b("oldest"));
            engine.put(b("gone"), b("x"));
            engine.put(b("stays"), b("s"));
            engine.flush();
            engine.put(b("k"), b("middle"));
            engine.delete(b("gone"));
            engine.flush();
            assertEquals(2, engine.tableCount(0));
            assertEquals("middle", s(engine.get(b("k"))));
            assertNull(engine.get(b("gone")), "a tombstone in a newer table hides the older value");
            assertEquals("s", s(engine.get(b("stays"))));

            engine.put(b("k"), b("newest"));
            engine.put(b("gone"), b("back"));
            assertEquals("newest", s(engine.get(b("k"))));
            assertEquals("back", s(engine.get(b("gone"))));

            engine.delete(b("stays"));
            assertNull(engine.get(b("stays")), "a tombstone in the MemTable hides a table value");
        }
    }

    @Test
    void theMemTableIsFlushedWhenItReachesItsSize() throws IOException {
        options.memTableSize(4096).level0StallTrigger(1000);
        try (LsmEngine engine = open()) {
            for (int i = 0; i < 400; i++) {
                engine.write(new WriteBatch().put(b(String.format("key-%04d", i)), new byte[100]), false);
            }
            assertTrue(engine.tableCount(0) >= 5, "tables: " + engine.tableCount(0));
            assertEquals(1, files(".wal").size(), "one log per MemTable; older logs are deleted");
            for (int i = 0; i < 400; i++) {
                assertEquals(100, engine.get(b(String.format("key-%04d", i))).length);
            }
        }
        try (LsmEngine engine = open()) {
            for (int i = 0; i < 400; i++) {
                assertEquals(100, engine.get(b(String.format("key-%04d", i))).length);
            }
            assertEquals(400, engine.lastSequence());
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void repeatedReadsAreServedFromTheBlockCache() throws IOException {
        try (LsmEngine engine = open()) {
            for (int i = 0; i < 200; i++) {
                engine.write(new WriteBatch().put(b("key-" + i), new byte[200]), false);
            }
            engine.flush();
            long missesBefore = engine.blockCache().misses();
            engine.get(b("key-7"));
            long missesAfterFirst = engine.blockCache().misses();
            assertEquals(missesBefore + 1, missesAfterFirst);
            long hitsBefore = engine.blockCache().hits();
            for (int i = 0; i < 10; i++) {
                assertEquals(200, engine.get(b("key-7")).length);
            }
            assertEquals(missesAfterFirst, engine.blockCache().misses());
            assertEquals(hitsBefore + 10, engine.blockCache().hits());
            assertEquals(0, engine.blockCache().pinnedCount(), "a finished read holds no block");
        }
    }

    @Test
    void theEngineWorksWithoutABlockCache() throws IOException {
        options.blockCacheCapacity(0);
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("1"));
            engine.flush();
            assertEquals("1", s(engine.get(b("a"))));
            assertEquals(0, engine.blockCache().size());
        }
    }

    // ---- reopen and crash ------------------------------------------------------------------

    @Test
    void dataSurvivesACleanReopen() throws IOException {
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("1"));
            engine.write(new WriteBatch().put(b("b"), b("2")).delete(b("a")), false);
        }
        try (LsmEngine engine = open()) {
            assertNull(engine.get(b("a")));
            assertEquals("2", s(engine.get(b("b"))));
            assertEquals(3, engine.lastSequence());
            assertEquals(1, engine.tableCount(0), "recovered log entries are written to a table");
            engine.put(b("c"), b("3"));
        }
        try (LsmEngine engine = open()) {
            assertEquals("3", s(engine.get(b("c"))));
            assertEquals("2", s(engine.get(b("b"))));
            assertEquals(4, engine.lastSequence());
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void theDirectoryHoldsExactlyTheLiveFiles() throws IOException {
        try (LsmEngine engine = open()) {
            assertEquals(List.of("000001.wal", "CURRENT", "LOCK", "MANIFEST-000002"), names());
            engine.put(b("a"), b("1"));
            engine.flush();
            assertEquals(List.of("000003.wal", "000004.sst", "CURRENT", "LOCK", "MANIFEST-000002"), names());
        }
        try (LsmEngine engine = open()) {
            // Nothing was in the log, so reopening adds no table; log and manifest are new.
            assertEquals(List.of("000004.sst", "000005.wal", "CURRENT", "LOCK", "MANIFEST-000006"), names());
            assertEquals("1", s(engine.get(b("a"))));
        }
    }

    @Test
    void aCrashKeepsSyncedWritesAndDropsUnsyncedOnes() throws IOException {
        LsmEngine engine = open();
        engine.put(b("synced"), b("1"));
        engine.write(new WriteBatch().put(b("unsynced"), b("2")), false);
        assertEquals("2", s(engine.get(b("unsynced"))));
        fs.crash();

        try (LsmEngine reopened = open()) {
            assertEquals("1", s(reopened.get(b("synced"))));
            assertNull(reopened.get(b("unsynced")));
            assertEquals(1, reopened.lastSequence());
            // The sequence number of the lost write is used again.
            reopened.put(b("next"), b("3"));
            assertEquals(2, reopened.lastSequence());
        }
    }

    @Test
    void anUnsyncedWriteBecomesDurableWithTheNextSyncedOne() throws IOException {
        LsmEngine engine = open();
        engine.write(new WriteBatch().put(b("first"), b("1")), false);
        engine.put(b("second"), b("2"));
        fs.crash();
        try (LsmEngine reopened = open()) {
            assertEquals("1", s(reopened.get(b("first"))));
            assertEquals("2", s(reopened.get(b("second"))));
        }
    }

    @Test
    void flushedDataSurvivesACrashWithoutAnyLog() throws IOException {
        LsmEngine engine = open();
        engine.write(new WriteBatch().put(b("a"), b("1")), false);
        engine.flush(); // a flush makes even unsynced writes durable
        engine.write(new WriteBatch().put(b("b"), b("2")), false);
        fs.crash();
        try (LsmEngine reopened = open()) {
            assertEquals("1", s(reopened.get(b("a"))));
            assertNull(reopened.get(b("b")));
            assertEquals(1, reopened.lastSequence());
        }
    }

    @Test
    void aCrashBetweenAnyTwoStepsOfAWorkloadLosesNothingAcknowledged() throws IOException {
        // Runs a fixed workload again and again, crashing one step later each time. Crashes
        // inside a single engine call belong to the crash tests of a later milestone.
        for (int crashAt = 0; crashAt < 17; crashAt++) {
            MemoryFileSystem crashFs = new MemoryFileSystem();
            int[] step = {0};
            int limit = crashAt;
            Runnable maybeCrash = () -> {
                if (step[0]++ == limit) {
                    crashFs.crash();
                    throw new CrashSignal();
                }
            };
            int acknowledged = 0;
            try {
                for (int round = 0; round < 3; round++) {
                    LsmEngine engine = LsmEngine.open(crashFs, dir, options);
                    maybeCrash.run();
                    engine.put(b("key-" + round + "-a"), b("v"));
                    acknowledged++;
                    maybeCrash.run();
                    engine.flush();
                    maybeCrash.run();
                    engine.put(b("key-" + round + "-b"), b("v"));
                    acknowledged++;
                    maybeCrash.run();
                    engine.close();
                    maybeCrash.run();
                }
            } catch (CrashSignal expected) {
                // The process died here.
            }
            try (LsmEngine engine = LsmEngine.open(crashFs, dir, options)) {
                assertEquals(acknowledged, engine.lastSequence(), "crash at step " + crashAt);
                int found = 0;
                for (int round = 0; round < 3; round++) {
                    found += engine.get(b("key-" + round + "-a")) != null ? 1 : 0;
                    found += engine.get(b("key-" + round + "-b")) != null ? 1 : 0;
                }
                assertEquals(acknowledged, found, "crash at step " + crashAt);
            }
        }
    }

    private static final class CrashSignal extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    // ---- damage ----------------------------------------------------------------------------

    @Test
    void aTornLogTailIsCutAndTheEngineOpens() throws IOException {
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("1"));
        }
        // Garbage after the last record, as an interrupted write would leave it.
        Path log = onlyLog();
        byte[] bytes = readAll(log);
        byte[] torn = Arrays.copyOf(bytes, bytes.length + 37);
        Arrays.fill(torn, bytes.length, torn.length, (byte) 0x5A);
        replaceFile(log, torn);

        try (LsmEngine engine = open()) {
            assertEquals("1", s(engine.get(b("a"))));
            engine.put(b("b"), b("2"));
        }
        try (LsmEngine engine = open()) {
            assertEquals("1", s(engine.get(b("a"))));
            assertEquals("2", s(engine.get(b("b"))));
        }
    }

    @Test
    void damageInSyncedLogDataPreventsOpening() throws IOException {
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("1"));
            engine.put(b("b"), b("2"));
            engine.put(b("c"), b("3")); // carries a sync point that covers the first two writes
        }
        Path log = onlyLog();
        byte[] bytes = readAll(log);
        bytes[20] ^= 0x01; // inside the first batch
        replaceFile(log, bytes);

        CorruptionException e = assertThrows(CorruptionException.class, this::open);
        assertTrue(e.file().endsWith(".wal"), e.getMessage());
        assertEquals(bytes.length, fs.size(log), "damage is never truncated away");
        assertEquals(0, fs.openHandleCount());
        // Opening again does not repair or hide anything.
        assertThrows(CorruptionException.class, this::open);
    }

    @Test
    void twoLogsLeftByAnInterruptedFlushAreBothReplayed() throws IOException {
        long sequence;
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("1"));
            sequence = engine.lastSequence();
        }
        // A newer log beside it, as a crash between two steps of a flush leaves it.
        Path log = onlyLog();
        long number = FileNames.parse(log).number();
        try (LogWriter writer = new LogWriter(fs.create(FileNames.wal(dir, number + 50)), number + 50, FileKind.WAL)) {
            writer.addRecord(new WriteBatch().put(b("b"), b("2")).encode(sequence + 1));
            writer.sync();
        }
        fs.syncDirectory(dir);
        try (LsmEngine engine = open()) {
            assertEquals("1", s(engine.get(b("a"))));
            assertEquals("2", s(engine.get(b("b"))));
        }
    }

    @Test
    void aSealedLogWithADamagedLastByteIsNotTreatedAsTorn() throws IOException {
        long sequence;
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("1"));
            sequence = engine.lastSequence();
        }
        Path log = onlyLog();
        long number = FileNames.parse(log).number();
        try (LogWriter writer = new LogWriter(fs.create(FileNames.wal(dir, number + 50)), number + 50, FileKind.WAL)) {
            writer.addRecord(new WriteBatch().put(b("b"), b("2")).encode(sequence + 1));
            writer.sync();
        }
        byte[] bytes = readAll(log);
        bytes[bytes.length - 1] ^= 0x01;
        replaceFile(log, bytes);

        CorruptionException e = assertThrows(CorruptionException.class, this::open);
        assertTrue(e.getMessage().contains("sealed"), e.getMessage());
    }

    /** Replaces the single, empty log of a fresh engine directory by crafted records. */
    private void craftLog(byte[]... records) throws IOException {
        open().close();
        Path log = onlyLog();
        long number = FileNames.parse(log).number();
        fs.delete(log);
        try (LogWriter writer = new LogWriter(fs.create(log), number, FileKind.WAL)) {
            for (byte[] record : records) {
                writer.addRecord(record);
            }
            writer.sync();
        }
        fs.syncDirectory(dir);
    }

    @Test
    void aGapInTheWriteHistoryPreventsOpening() throws IOException {
        craftLog(new WriteBatch().put(b("a"), b("1")).encode(1),
                new WriteBatch().put(b("b"), b("2")).encode(3)); // 2 is missing
        CorruptionException e = assertThrows(CorruptionException.class, this::open);
        assertTrue(e.getMessage().contains("expected 2"), e.getMessage());
    }

    @Test
    void aMalformedBatchPreventsOpening() throws IOException {
        byte[] batch = new WriteBatch().put(b("a"), b("1")).encode(1);
        LittleEndian.putUint32(batch, 13, 500); // key length beyond the record
        craftLog(batch);
        assertThrows(CorruptionException.class, this::open);
    }

    @Test
    void aDamagedTableFilePreventsOpening() throws IOException {
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("1"));
            engine.flush();
        }
        Path table = files(".sst").get(0);
        byte[] bytes = readAll(table);
        bytes[bytes.length - 20] ^= 0x01; // footer
        replaceFile(table, bytes);
        CorruptionException e = assertThrows(CorruptionException.class, this::open);
        assertTrue(e.file().endsWith(".sst"), e.getMessage());
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void aDamagedDataBlockFailsTheReadThatNeedsIt() throws IOException {
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("1"));
            engine.flush();
        }
        Path table = files(".sst").get(0);
        byte[] bytes = readAll(table);
        bytes[5] ^= 0x01; // first data block
        replaceFile(table, bytes);
        try (LsmEngine engine = open()) {
            CorruptionException e = assertThrows(CorruptionException.class, () -> engine.get(b("a")));
            assertEquals(0, e.offset());
            assertNull(engine.get(b("zzz")), "a key outside the table's range does not touch it");
        }
    }

    @Test
    void aMissingOrTruncatedTableFilePreventsOpening() throws IOException {
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("1"));
            engine.flush();
        }
        Path table = files(".sst").get(0);
        byte[] bytes = readAll(table);
        replaceFile(table, Arrays.copyOf(bytes, bytes.length - 1));
        assertThrows(CorruptionException.class, this::open);

        fs.delete(table);
        CorruptionException e = assertThrows(CorruptionException.class, this::open);
        assertTrue(e.getMessage().contains("missing"), e.getMessage());
    }

    @Test
    void aDamagedOrMissingCurrentFilePreventsOpening() throws IOException {
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("1"));
        }
        Path current = dir.resolve(FileNames.CURRENT);
        byte[] bytes = readAll(current);

        byte[] damaged = bytes.clone();
        damaged[20] ^= 0x01;
        replaceFile(current, damaged);
        assertThrows(CorruptionException.class, this::open);

        // A newer format version with a valid checksum is not damage.
        String head = "FREYJA CURRENT 2\nMANIFEST-000002\n";
        int crc = io.github.stefan_zobel.freyja.common.Checksums.crc32c(b(head), 0, head.length());
        replaceFile(current, b(head + String.format("crc32c=%08x\n", crc)));
        assertThrows(UnsupportedFormatException.class, this::open);

        fs.delete(current);
        CorruptionException e = assertThrows(CorruptionException.class, this::open);
        assertTrue(e.getMessage().contains("CURRENT is missing"), e.getMessage());

        replaceFile(current, bytes);
        try (LsmEngine engine = open()) {
            assertEquals("1", s(engine.get(b("a"))));
        }
    }

    @Test
    void aDamagedOrMissingManifestPreventsOpening() throws IOException {
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("1"));
            engine.flush();
            engine.put(b("b"), b("2"));
            engine.flush();
        }
        long number = CurrentFile.read(fs, dir);
        Path manifest = FileNames.manifest(dir, number);
        byte[] bytes = readAll(manifest);

        byte[] damaged = bytes.clone();
        damaged[25] ^= 0x01; // inside the first edit, which later sync points cover
        replaceFile(manifest, damaged);
        assertThrows(CorruptionException.class, this::open);

        fs.delete(manifest);
        CorruptionException e = assertThrows(CorruptionException.class, this::open);
        assertTrue(e.getMessage().contains("manifest named by CURRENT is missing"), e.getMessage());

        replaceFile(manifest, bytes);
        try (LsmEngine engine = open()) {
            assertEquals("1", s(engine.get(b("a"))));
            assertEquals("2", s(engine.get(b("b"))));
        }
    }

    @Test
    void aTornManifestTailLosesOnlyTheEditThatNeverTookEffect() throws IOException {
        LsmEngine engine = open();
        engine.put(b("a"), b("1"));
        engine.flush();
        engine.close();
        long number = CurrentFile.read(fs, dir);
        Path manifest = FileNames.manifest(dir, number);
        byte[] bytes = readAll(manifest);
        byte[] torn = Arrays.copyOf(bytes, bytes.length + 20);
        Arrays.fill(torn, bytes.length, torn.length, (byte) 0x33);
        replaceFile(manifest, torn);
        try (LsmEngine reopened = open()) {
            assertEquals("1", s(reopened.get(b("a"))));
        }
    }

    // ---- directory handling ----------------------------------------------------------------

    @Test
    void orphansAndTemporaryFilesAreRemovedAtOpen() throws IOException {
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("1"));
            engine.flush();
        }
        for (String name : new String[] {"000900.sst", "MANIFEST-000901", "CURRENT.tmp", "000902.sst.tmp"}) {
            try (WritableFile file = fs.create(dir.resolve(name))) {
                file.append(new byte[] {1, 2, 3}, 0, 3);
            }
        }
        try (WritableFile file = fs.create(dir.resolve("notes.txt"))) {
            file.append(new byte[] {1, 2, 3}, 0, 3);
        }
        try (LsmEngine engine = open()) {
            assertEquals("1", s(engine.get(b("a"))));
            List<String> names = names();
            assertFalse(names.contains("000900.sst"));
            assertFalse(names.contains("MANIFEST-000901"));
            assertFalse(names.contains("CURRENT.tmp"));
            assertFalse(names.contains("000902.sst.tmp"));
            assertTrue(names.contains("notes.txt"), "files the engine does not know are left alone");
            // File numbers continue above the highest number ever seen in the directory.
            assertTrue(names.contains("000902.wal"), names.toString());
        }
    }

    @Test
    void aDirectoryWithDataButWithoutCurrentIsRejected() throws IOException {
        fs.createDirectories(dir);
        try (WritableFile file = fs.create(dir.resolve("000001.wal"))) {
            file.append(new byte[] {1}, 0, 1);
        }
        assertThrows(CorruptionException.class, this::open);
    }

    @Test
    void aSecondEngineCannotOpenTheSameDirectory() throws IOException {
        try (LsmEngine engine = open()) {
            IOException e = assertThrows(IOException.class, this::open);
            assertTrue(e.getMessage().contains("lock"), e.getMessage());
            engine.put(b("a"), b("1")); // the first engine is unaffected
        }
        open().close(); // the lock is released on close
        assertEquals(0, fs.openHandleCount());
    }

    // ---- failure ---------------------------------------------------------------------------

    @Test
    void aFailedSyncStopsTheEngineForGood() throws IOException {
        HookedFileSystem hooked = new HookedFileSystem(fs);
        LsmEngine engine = LsmEngine.open(hooked, dir, options);
        engine.put(b("a"), b("1"));

        hooked.failSyncs = true;
        assertThrows(EngineFailedException.class, () -> engine.put(b("b"), b("2")));
        assertNull(engine.get(b("b")), "a write that failed is not visible");

        hooked.failSyncs = false; // the sync is never retried
        EngineFailedException e = assertThrows(EngineFailedException.class, () -> engine.put(b("c"), b("3")));
        assertTrue(e.getCause() instanceof EngineFailedException);
        assertThrows(EngineFailedException.class, engine::flush);
        assertEquals("1", s(engine.get(b("a"))), "reads keep working");
        engine.close();
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void aFailedAppendStopsTheEngine() throws IOException {
        HookedFileSystem hooked = new HookedFileSystem(fs);
        LsmEngine engine = LsmEngine.open(hooked, dir, options);
        hooked.failAppends = true;
        assertThrows(EngineFailedException.class, () -> engine.write(new WriteBatch().put(b("a"), b("1")), false));
        hooked.failAppends = false;
        assertThrows(EngineFailedException.class, () -> engine.put(b("b"), b("2")));
        engine.close();
    }

    @Test
    void aFailedFlushStopsTheEngineButLosesNothing() throws IOException {
        HookedFileSystem hooked = new HookedFileSystem(fs);
        LsmEngine engine = LsmEngine.open(hooked, dir, options);
        engine.put(b("a"), b("1"));
        hooked.failSyncs = true;
        assertThrows(EngineFailedException.class, engine::flush);
        hooked.failSyncs = false;
        assertEquals("1", s(engine.get(b("a"))));
        engine.close();
        assertEquals(0, fs.openHandleCount());

        try (LsmEngine reopened = open()) {
            assertEquals("1", s(reopened.get(b("a"))));
        }
    }

    @Test
    void aClosedEngineRejectsWrites() throws IOException {
        LsmEngine engine = open();
        engine.put(b("a"), b("1"));
        engine.close();
        engine.close();
        IOException e = assertThrows(IOException.class, () -> engine.put(b("b"), b("2")));
        assertFalse(e instanceof EngineFailedException);
        assertThrows(IOException.class, engine::flush);
    }

    // ---- group commit ----------------------------------------------------------------------

    @Test
    void concurrentWritersAreCommittedInGroups() throws Exception {
        HookedFileSystem hooked = new HookedFileSystem(fs);
        hooked.syncDelayMillis = 2;
        options.memTableSize(16 * 1024); // flushes happen while writers and readers run
        int threads = 16;
        int writesPerThread = 40;
        AtomicReference<Throwable> failure = new AtomicReference<>();

        try (LsmEngine engine = LsmEngine.open(hooked, dir, options)) {
            int syncsBefore = hooked.syncCount.get();
            List<Thread> workers = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int id = t;
                Thread worker = new Thread(() -> {
                    try {
                        for (int i = 0; i < writesPerThread; i++) {
                            engine.write(new WriteBatch()
                                    .put(b("t" + id + "-" + i), b("v" + i))
                                    .put(b("last-" + id), b("" + i)), true);
                            // A write that returned is visible at once, also across flushes.
                            if (!("v" + i).equals(s(engine.get(b("t" + id + "-" + i))))) {
                                throw new AssertionError("own write not visible");
                            }
                            if (!("" + i).equals(s(engine.get(b("last-" + id))))) {
                                throw new AssertionError("own overwrite not visible");
                            }
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    }
                });
                workers.add(worker);
                worker.start();
            }
            for (Thread worker : workers) {
                worker.join();
            }
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            int writes = threads * writesPerThread;
            assertEquals(2L * writes, engine.lastSequence());
            assertTrue(engine.tableCount(0) > 0, "the workload was meant to flush");
            // Flushes add a few syncs of their own; grouping still keeps the total far below
            // one sync per write.
            int syncs = hooked.syncCount.get() - syncsBefore;
            assertTrue(syncs < writes, syncs + " syncs for " + writes + " writes: no grouping");
        }

        try (LsmEngine engine = open()) {
            assertEquals(2L * threads * writesPerThread, engine.lastSequence());
            for (int t = 0; t < threads; t++) {
                assertEquals("" + (writesPerThread - 1), s(engine.get(b("last-" + t))));
                for (int i = 0; i < writesPerThread; i++) {
                    assertEquals("v" + i, s(engine.get(b("t" + t + "-" + i))));
                }
            }
        }
    }

    @Test
    void aGroupTakesAtMostTheConfiguredNumberOfBytes() throws Exception {
        // With a tiny group limit every batch is committed alone; the result is the same.
        options.maxGroupCommitSize(1);
        try (LsmEngine engine = open()) {
            List<Thread> workers = new ArrayList<>();
            for (int t = 0; t < 4; t++) {
                int id = t;
                Thread worker = new Thread(() -> {
                    for (int i = 0; i < 50; i++) {
                        try {
                            engine.write(new WriteBatch().put(b("k" + id + "-" + i), b("v")), i % 2 == 0);
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    }
                });
                workers.add(worker);
                worker.start();
            }
            for (Thread worker : workers) {
                worker.join();
            }
            assertEquals(200, engine.lastSequence());
        }
        try (LsmEngine engine = open()) {
            assertEquals(200, engine.lastSequence());
        }
    }
}
