package io.github.stefan_zobel.freyja.lsm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.lsm.fs.MemoryFileSystem;
import io.github.stefan_zobel.freyja.lsm.fs.ReadableFile;
import io.github.stefan_zobel.freyja.lsm.fs.WritableFile;

/** Compaction: levels, retention, and the lifetime of table files. */
class LsmEngineCompactionTest {

    private final MemoryFileSystem fs = new MemoryFileSystem();
    private final Path dir = Path.of("compaction-test").toAbsolutePath();
    private final EngineOptions options = new EngineOptions().deterministic(true)
            .blockCacheCapacity(1 << 20)
            .level0CompactionTrigger(2);

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String s(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    private static String key(int i) {
        return String.format("key-%05d", i);
    }

    private LsmEngine open() throws IOException {
        return LsmEngine.open(fs, dir, options);
    }

    private List<String> tableFiles() throws IOException {
        List<String> result = new ArrayList<>();
        for (Path path : fs.list(dir)) {
            if (path.getFileName().toString().endsWith(".sst")) {
                result.add(path.getFileName().toString());
            }
        }
        return result;
    }

    private long tableBytes() throws IOException {
        long total = 0;
        for (Path path : fs.list(dir)) {
            if (path.getFileName().toString().endsWith(".sst")) {
                total += fs.size(path);
            }
        }
        return total;
    }

    private static List<String> collect(ScanIterator scan) throws IOException {
        List<String> result = new ArrayList<>();
        try (scan) {
            for (; scan.valid(); scan.next()) {
                result.add(s(scan.key()) + "=" + s(scan.value()));
            }
        }
        return result;
    }

    // ---- basic compaction ------------------------------------------------------------------

    @Test
    void nothingIsCompactedBelowTheTrigger() throws IOException {
        try (LsmEngine engine = open()) {
            assertFalse(engine.compact());
            engine.put(b("a"), b("1"));
            engine.flush();
            assertFalse(engine.compact());
            assertEquals(1, engine.tableCount(0));
        }
    }

    @Test
    void levelZeroIsMergedIntoLevelOne() throws IOException {
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("a1"));
            engine.put(b("c"), b("c1"));
            engine.flush();
            engine.put(b("b"), b("b2"));
            engine.put(b("c"), b("c2"));
            engine.delete(b("a"));
            engine.flush();
            assertEquals(2, engine.tableCount(0));
            List<String> before = tableFiles();

            assertTrue(engine.compact());
            assertEquals(0, engine.tableCount(0));
            assertEquals(1, engine.tableCount(1));
            assertEquals(1, tableFiles().size(), "the inputs are deleted");
            assertFalse(before.contains(tableFiles().get(0)));

            assertNull(engine.get(b("a")));
            assertEquals("b2", s(engine.get(b("b"))));
            assertEquals("c2", s(engine.get(b("c"))));
            assertEquals(List.of("b=b2", "c=c2"), collect(engine.scan(null, null)));
            assertFalse(engine.compact(), "nothing left to do");
        }
        try (LsmEngine engine = open()) {
            assertEquals(1, engine.tableCount(1), "the levels survive a reopen");
            assertEquals(List.of("b=b2", "c=c2"), collect(engine.scan(null, null)));
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void overwrittenAndDeletedDataIsReclaimed() throws IOException {
        try (LsmEngine engine = open()) {
            byte[] value = new byte[1000];
            for (int round = 0; round < 6; round++) {
                for (int i = 0; i < 50; i++) {
                    engine.write(new WriteBatch().put(b(key(i)), value), false);
                }
                engine.flush();
            }
            long sixVersions = tableBytes();
            assertTrue(engine.compact());
            long oneVersion = tableBytes();
            assertTrue(oneVersion < sixVersions / 4, oneVersion + " of " + sixVersions + " bytes remain");
            for (int i = 0; i < 50; i++) {
                assertEquals(1000, engine.get(b(key(i))).length);
            }

            // Delete everything. Once the tombstones reach the data, both disappear.
            for (int i = 0; i < 50; i++) {
                engine.write(new WriteBatch().delete(b(key(i))), false);
            }
            engine.flush();
            engine.put(b("zzz"), b("keep"));
            engine.flush();
            assertTrue(engine.compact());
            assertEquals(List.of("zzz=keep"), collect(engine.scan(null, null)));
            assertTrue(tableBytes() < 1000, "table bytes left: " + tableBytes());
        }
    }

    @Test
    void aCompactionWhoseOutputIsEmptyLeavesNoFile() throws IOException {
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("1"));
            engine.flush();
            engine.delete(b("a"));
            engine.flush();
            assertTrue(engine.compact());
            assertEquals(0, engine.tableCount(0));
            assertEquals(0, engine.tableCount(1));
            assertEquals(List.of(), tableFiles());
            assertNull(engine.get(b("a")));
        }
        try (LsmEngine engine = open()) {
            assertNull(engine.get(b("a")));
            assertEquals(2, engine.lastSequence());
        }
    }

    @Test
    void dataMovesThroughSeveralLevels() throws IOException {
        options.level1TargetSize(20_000).targetFileSize(8_000).memTableSize(8_000).levelCount(4);
        try (LsmEngine engine = open()) {
            for (int round = 0; round < 4; round++) {
                for (int i = 0; i < 600; i++) {
                    engine.write(new WriteBatch().put(b(key(i)), b("round-" + round + "-" + "x".repeat(60))), false);
                    if (i % 100 == 99) {
                        engine.compact();
                    }
                }
            }
            engine.flush();
            engine.compact();
            assertTrue(engine.tableCount(2) + engine.tableCount(3) > 0,
                    "levels: " + engine.tableCount(0) + " " + engine.tableCount(1) + " "
                            + engine.tableCount(2) + " " + engine.tableCount(3));
            assertTrue(tableFiles().size() > 2, "the data is spread over several files: " + tableFiles());
            for (int i = 0; i < 600; i++) {
                assertTrue(s(engine.get(b(key(i)))).startsWith("round-3-"), key(i));
            }
            List<String> all = collect(engine.scan(null, null));
            assertEquals(600, all.size());
            assertTrue(all.get(599).startsWith(key(599) + "=round-3-"));
        }
        try (LsmEngine engine = open()) {
            for (int i = 0; i < 600; i += 37) {
                assertTrue(s(engine.get(b(key(i)))).startsWith("round-3-"), key(i));
            }
        }
        assertEquals(0, fs.openHandleCount());
    }

    // ---- snapshots and open scans ----------------------------------------------------------

    @Test
    void aSnapshotKeepsItsVersionsThroughCompaction() throws IOException {
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("old"));
            engine.put(b("gone"), b("x"));
            engine.flush();
            Snapshot snapshot = engine.snapshot();
            engine.put(b("a"), b("new"));
            engine.delete(b("gone"));
            engine.flush();
            assertTrue(engine.compact());

            assertEquals("old", s(engine.get(b("a"), snapshot)));
            assertEquals("x", s(engine.get(b("gone"), snapshot)));
            assertEquals(List.of("a=old", "gone=x"), collect(engine.scan(null, null, snapshot)));
            assertEquals("new", s(engine.get(b("a"))));
            assertNull(engine.get(b("gone")));

            // Without the snapshot the old versions are no longer needed.
            long withSnapshot = tableBytes();
            snapshot.close();
            engine.put(b("b"), b("1"));
            engine.flush();
            engine.put(b("c"), b("1"));
            engine.flush();
            assertTrue(engine.compact());
            assertEquals(List.of("a=new", "b=1", "c=1"), collect(engine.scan(null, null)));
            assertTrue(tableBytes() <= withSnapshot + 100);
        }
    }

    @Test
    void anOpenScanKeepsItsTablesUntilItIsClosed() throws IOException {
        try (LsmEngine engine = open()) {
            for (int i = 0; i < 100; i++) {
                engine.write(new WriteBatch().put(b(key(i)), b("v1")), false);
            }
            engine.flush();
            for (int i = 0; i < 100; i++) {
                engine.write(new WriteBatch().put(b(key(i)), b("v2")), false);
            }
            engine.flush();
            List<String> inputs = tableFiles();

            ScanIterator scan = engine.scan(null, null);
            assertTrue(engine.compact());
            assertTrue(tableFiles().containsAll(inputs), "inputs stay while a scan uses them");
            assertEquals(3, tableFiles().size());

            int seen = 0;
            for (; scan.valid(); scan.next()) {
                assertEquals("v2", s(scan.value()));
                seen++;
            }
            assertEquals(100, seen);
            scan.close();
            assertEquals(1, tableFiles().size(), "the inputs are deleted with the last reader");
            assertEquals(0, engine.blockCache().pinnedCount());
            assertEquals("v2", s(engine.get(b(key(5)))));
        }
    }

    // ---- crash and damage ------------------------------------------------------------------

    @Test
    void aCrashRightAfterACompactionLosesNothing() throws IOException {
        LsmEngine engine = open();
        for (int round = 0; round < 3; round++) {
            for (int i = 0; i < 30; i++) {
                engine.put(b(key(i)), b("round-" + round));
            }
            engine.flush();
        }
        assertTrue(engine.compact());
        fs.crash();
        try (LsmEngine reopened = open()) {
            assertEquals(90, reopened.lastSequence());
            for (int i = 0; i < 30; i++) {
                assertEquals("round-2", s(reopened.get(b(key(i)))));
            }
            assertEquals(1, tableFiles().size());
        }
    }

    @Test
    void aDamagedInputAbortsTheCompactionAndIsNotCopied() throws IOException {
        try (LsmEngine engine = open()) {
            for (int i = 0; i < 200; i++) {
                engine.write(new WriteBatch().put(b(key(i)), new byte[100]), false);
            }
            engine.flush();
            engine.put(b("other"), b("1"));
            engine.flush();
        }
        List<String> files = tableFiles();
        Path damagedTable = dir.resolve(files.get(0));
        byte[] bytes;
        try (ReadableFile file = fs.open(damagedTable)) {
            bytes = new byte[(int) file.size()];
            file.readFully(0, bytes, 0, bytes.length);
        }
        bytes[9000] ^= 0x01; // a data block in the middle
        fs.delete(damagedTable);
        try (WritableFile file = fs.create(damagedTable)) {
            file.append(bytes, 0, bytes.length);
            file.sync();
        }

        try (LsmEngine engine = open()) {
            CorruptionException e = assertThrows(CorruptionException.class, engine::compact);
            assertTrue(e.file().endsWith(files.get(0)), e.getMessage());
            assertEquals(files, tableFiles(), "no output is left behind and no input is removed");
            assertEquals(2, engine.tableCount(0));
            // The engine keeps working; the damage is still there to be found.
            assertEquals("1", s(engine.get(b("other"))));
            assertEquals(100, engine.get(b(key(0))).length);
            engine.put(b("more"), b("2"));
            assertThrows(CorruptionException.class, engine::compact);
            assertEquals(0, engine.blockCache().pinnedCount());
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void aFailedOutputWriteAbortsTheCompactionAndLosesNothing() throws IOException {
        HookedFileSystem hooked = new HookedFileSystem(fs);
        LsmEngine engine = LsmEngine.open(hooked, dir, options);
        engine.put(b("a"), b("1"));
        engine.flush();
        engine.put(b("b"), b("2"));
        engine.flush();
        List<String> files = tableFiles();

        hooked.failSyncs = true;
        assertThrows(IOException.class, engine::compact);
        hooked.failSyncs = false;
        assertEquals(files, tableFiles(), "the unfinished output is removed");
        assertEquals("1", s(engine.get(b("a"))));
        // The failure hit only a new file that nothing refers to; the engine goes on.
        engine.put(b("c"), b("3"));
        assertTrue(engine.compact());
        assertEquals("3", s(engine.get(b("c"))));
        engine.close();
        assertEquals(0, fs.openHandleCount());

        try (LsmEngine reopened = open()) {
            assertEquals("1", s(reopened.get(b("a"))));
            assertEquals("2", s(reopened.get(b("b"))));
        }
    }
}
