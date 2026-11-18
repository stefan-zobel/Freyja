package io.github.sz.freyja.lsm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.lsm.fs.MemoryFileSystem;
import io.github.sz.freyja.lsm.manifest.CurrentFile;
import io.github.sz.freyja.lsm.manifest.FileNames;
import io.github.sz.freyja.lsm.table.TableMetadataBudget;

/**
 * Bounded resources of the engine: the table-metadata budget, the size of the manifest, and
 * the I/O rate of compaction.
 */
class LsmEngineResourceTest {

    private final MemoryFileSystem fs = new MemoryFileSystem();
    private final Path dir = Path.of("resource-test").toAbsolutePath();
    private final EngineOptions options = new EngineOptions().reserveSize(0).minFreeSpace(0)
            .deterministic(true)
            .blockCacheCapacity(1 << 20)
            .level0StallTrigger(10_000)
            .level0CompactionTrigger(10_000);

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String s(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    private List<String> manifests() throws IOException {
        List<String> result = new ArrayList<>();
        for (Path path : fs.list(dir)) {
            if (path.getFileName().toString().startsWith("MANIFEST-")) {
                result.add(path.getFileName().toString());
            }
        }
        return result;
    }

    // ---- table metadata --------------------------------------------------------------------

    @Test
    void indexAndFilterBlocksStayWithinTheirBudget() throws IOException {
        // Each table of this test needs about 100 bytes for its index and filter block.
        options.tableMetadataBudget(250);
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            for (int table = 0; table < 20; table++) {
                engine.put(b("key-" + table), b("value-" + table));
                engine.flush();
            }
            TableMetadataBudget budget = engine.tableMetadataBudget();
            assertEquals(20, engine.tableCount(0));
            assertTrue(budget.usedBytes() <= 250, "used: " + budget.usedBytes());
            assertTrue(budget.loadedCount() < 20 && budget.loadedCount() >= 1, "loaded: " + budget.loadedCount());
            assertTrue(budget.drops() >= 17, "drops: " + budget.drops());

            // Every table still answers; its blocks are loaded again when needed.
            long dropsBefore = budget.drops();
            for (int round = 0; round < 3; round++) {
                for (int table = 0; table < 20; table++) {
                    assertEquals("value-" + table, s(engine.get(b("key-" + table))));
                }
            }
            assertTrue(budget.drops() > dropsBefore);
            assertTrue(budget.usedBytes() <= 250);

            // A scan uses all tables at once. It keeps what it has loaded, and it works.
            int count = 0;
            try (ScanIterator scan = engine.scan(null, null)) {
                for (; scan.valid(); scan.next()) {
                    count++;
                }
            }
            assertEquals(20, count);
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void aBudgetSmallerThanOneTableStillWorks() throws IOException {
        options.tableMetadataBudget(0);
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            for (int table = 0; table < 5; table++) {
                engine.put(b("key-" + table), b("value-" + table));
                engine.flush();
            }
            for (int table = 0; table < 5; table++) {
                assertEquals("value-" + table, s(engine.get(b("key-" + table))));
            }
            assertEquals(1, engine.tableMetadataBudget().loadedCount(), "only the table used last");
        }
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            assertEquals("value-3", s(engine.get(b("key-3"))));
        }
    }

    @Test
    void closedTablesGiveTheirBudgetBack() throws IOException {
        options.level0CompactionTrigger(2);
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            engine.put(b("a"), b("1"));
            engine.flush();
            engine.put(b("b"), b("2"));
            engine.flush();
            assertEquals(2, engine.tableMetadataBudget().loadedCount());
            assertTrue(engine.compact());
            assertEquals(1, engine.tableMetadataBudget().loadedCount());
        }
    }

    // ---- manifest --------------------------------------------------------------------------

    @Test
    void aLargeManifestIsReplacedByOneThatHoldsOnlyTheCurrentState() throws IOException {
        options.maxManifestSize(600);
        long first;
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            first = engine.manifestNumber();
            for (int i = 0; i < 40; i++) {
                engine.put(b("key-" + i), b("value-" + i));
                engine.flush();
            }
            assertNotEquals(first, engine.manifestNumber(), "the manifest was replaced");
            assertEquals(List.of(FileNames.manifestName(engine.manifestNumber())), manifests(),
                    "the old manifest is deleted");
            assertEquals(engine.manifestNumber(), CurrentFile.read(fs, dir));
            assertTrue(fs.size(FileNames.manifest(dir, engine.manifestNumber())) < 6000);
            for (int i = 0; i < 40; i++) {
                assertEquals("value-" + i, s(engine.get(b("key-" + i))));
            }
        }
        // The replaced manifest describes the same state.
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            assertEquals(40, engine.tableCount(0));
            assertEquals(40, engine.lastSequence());
            for (int i = 0; i < 40; i++) {
                assertEquals("value-" + i, s(engine.get(b("key-" + i))));
            }
        }
    }

    @Test
    void aCrashRightAfterAManifestReplacementLosesNothing() throws IOException {
        options.maxManifestSize(300);
        LsmEngine engine = LsmEngine.open(fs, dir, options);
        long first = engine.manifestNumber();
        int written = 0;
        while (engine.manifestNumber() == first) {
            engine.put(b("key-" + written), b("value-" + written));
            engine.flush();
            written++;
        }
        fs.crash();
        try (LsmEngine reopened = LsmEngine.open(fs, dir, options)) {
            assertEquals(written, reopened.lastSequence());
            for (int i = 0; i < written; i++) {
                assertEquals("value-" + i, s(reopened.get(b("key-" + i))));
            }
        }
    }

    @Test
    void aFailedManifestReplacementStopsTheEngineAndLosesNothing() throws IOException {
        options.maxManifestSize(300);
        LsmEngine engine = LsmEngine.open(fs, dir, options);
        long first = engine.manifestNumber();
        // Occupy the names the next manifests could get, so that creating one fails.
        for (long number = first + 1; number < first + 200; number++) {
            fs.create(FileNames.manifest(dir, number)).close();
        }
        int flushed = 0;
        EngineFailedException failed = null;
        while (failed == null && flushed < 100) {
            engine.put(b("key-" + flushed), b("value-" + flushed));
            try {
                engine.flush();
            } catch (EngineFailedException e) {
                failed = e;
            }
            flushed++;
        }
        assertTrue(failed != null, "the manifest should have been due for replacement");
        assertEquals(first, engine.manifestNumber());
        assertThrows(EngineFailedException.class, () -> engine.put(b("x"), b("y")));
        assertEquals("value-0", s(engine.get(b("key-0"))), "reads keep working");
        engine.close();

        // The old manifest is still the one CURRENT names, and it holds every edit.
        try (LsmEngine reopened = LsmEngine.open(fs, dir, options)) {
            assertEquals(flushed, reopened.lastSequence());
            for (int i = 0; i < flushed; i++) {
                assertEquals("value-" + i, s(reopened.get(b("key-" + i))));
            }
        }
        assertEquals(0, fs.openHandleCount());
    }

    // ---- I/O rate --------------------------------------------------------------------------

    @Test
    void compactionIsHeldToItsRateInNormalMode() throws Exception {
        EngineOptions normal = new EngineOptions().reserveSize(0).minFreeSpace(0)
                .blockCacheCapacity(1 << 20)
                .level0CompactionTrigger(4)
                .compactionBytesPerSecond(400_000);
        try (LsmEngine engine = LsmEngine.open(fs, dir, normal)) {
            for (int file = 0; file < 4; file++) {
                for (int i = 0; i < 100; i++) {
                    engine.write(new WriteBatch().put(b("key-" + file + "-" + i), new byte[200]), false);
                }
                engine.flush();
            }
            long deadline = System.nanoTime() + 10_000_000_000L;
            while (engine.tableCount(0) >= 4 && System.nanoTime() < deadline) {
                Thread.sleep(2);
            }
            assertEquals(0, engine.tableCount(0), "the background compaction ran");
            // 400 entries of about 220 bytes at 400 kB per second: about 200 ms of waiting, less
            // what the compaction takes by itself. The bounds are wide, because a slow machine
            // needs less waiting to stay below the rate and sleeps longer than it is asked to.
            long throttled = engine.compactionThrottledNanos();
            assertTrue(throttled > 40_000_000L && throttled < 2_000_000_000L, "throttled for " + throttled + " ns");
        }
    }

    @Test
    void deterministicModeNeverWaitsForTheRate() throws IOException {
        options.compactionBytesPerSecond(1).level0CompactionTrigger(2);
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            engine.put(b("a"), new byte[1000]);
            engine.flush();
            engine.put(b("b"), new byte[1000]);
            engine.flush();
            assertTrue(engine.compact()); // at one byte per second this would take half an hour
            assertEquals(0, engine.compactionThrottledNanos());
            assertFalse(engine.compact());
        }
    }
}
