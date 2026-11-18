package io.github.stefan_zobel.freyja.lsm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.common.ByteComparator;
import io.github.stefan_zobel.freyja.common.Metrics;
import io.github.stefan_zobel.freyja.common.testing.Seeds;
import io.github.stefan_zobel.freyja.lsm.fs.MemoryFileSystem;
import io.github.stefan_zobel.freyja.lsm.fs.ReadableFile;
import io.github.stefan_zobel.freyja.lsm.log.FileKind;
import io.github.stefan_zobel.freyja.lsm.log.LogReader;
import io.github.stefan_zobel.freyja.lsm.manifest.FileNames;

/**
 * Damage to stored data, a full disk, scrubbing, and the figures the engine reports
 * (docs/spec/verification.md, section 2.2; docs/spec/storage-engine.md, section 3).
 */
class LsmEngineFaultTest {

    private final Path dir = Path.of("fault-test", "db").toAbsolutePath();

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String s(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    private static EngineOptions smallOptions() {
        return new EngineOptions()
                .deterministic(true)
                .reserveSize(0)
                .minFreeSpace(0)
                .blockCacheCapacity(64 * 1024)
                .memTableSize(3000)
                .level0CompactionTrigger(2)
                .level1TargetSize(8000)
                .targetFileSize(3000)
                .levelCount(4);
    }

    /** A sink that keeps everything it is told. */
    private static final class RecordingMetrics implements Metrics {
        final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
        final Map<String, Long> gauges = new ConcurrentHashMap<>();
        final Map<String, AtomicLong> durations = new ConcurrentHashMap<>();

        @Override
        public void count(String name, long delta) {
            counters.computeIfAbsent(name, k -> new AtomicLong()).addAndGet(delta);
        }

        @Override
        public void gauge(String name, long value) {
            gauges.put(name, value);
        }

        @Override
        public void duration(String name, long nanos) {
            durations.computeIfAbsent(name, k -> new AtomicLong()).incrementAndGet();
        }

        long counter(String name) {
            AtomicLong value = counters.get(name);
            return value == null ? 0 : value.get();
        }
    }

    // ---- damage to synced data -------------------------------------------------------------

    /**
     * Builds a small database with tables in several levels and synced writes in the log,
     * and returns what it holds.
     */
    private TreeMap<byte[], byte[]> build(MemoryFileSystem fs, long seed) throws IOException {
        Random random = new Random(seed);
        TreeMap<byte[], byte[]> model = new TreeMap<>(ByteComparator.UNSIGNED);
        try (LsmEngine engine = LsmEngine.open(fs, dir, smallOptions())) {
            for (int i = 0; i < 160; i++) {
                byte[] key = b("key-" + random.nextInt(60));
                if (random.nextInt(5) == 0) {
                    engine.delete(key);
                    model.remove(key);
                } else {
                    byte[] value = new byte[random.nextInt(200)];
                    random.nextBytes(value);
                    engine.put(key, value);
                    model.put(key, value);
                }
                if (i % 23 == 22 && i < 140) {
                    engine.compact();
                }
            }
        }
        return model;
    }

    /** Returns how many bytes of a log later sync points prove to have been synced. */
    private static long provenSynced(MemoryFileSystem fs, Path path, FileKind kind) throws IOException {
        long[] proven = {0};
        try (ReadableFile file = fs.open(path)) {
            LogReader.read(file, path.toString(), FileNames.parse(path).number(), kind, false,
                    new LogReader.Listener() {
                        @Override
                        public void record(long offset, byte[] payload) {
                        }

                        @Override
                        public void syncPoint(long offset, long syncedLength) {
                            proven[0] = Math.max(proven[0], syncedLength);
                        }
                    });
        }
        return proven[0];
    }

    @Test
    void damageToSyncedDataIsDetectedAndNeverAnsweredWithWrongData() {
        int[] byKind = new int[3]; // tables, logs and manifest, CURRENT
        Seeds.forEach(LsmEngineFaultTest.class, "engine-damage", 400, seed -> {
            Random random = new Random(seed * 31);
            MemoryFileSystem fs = new MemoryFileSystem();
            TreeMap<byte[], byte[]> model = build(fs, seed % 7);

            // Choose a file and a range in which every byte is covered by a checksum and,
            // for logs, proven to be synced. The unproven tail of the last log is the
            // residual risk stated in the format specification.
            List<Path> candidates = new ArrayList<>();
            List<Long> limits = new ArrayList<>();
            for (Path path : fs.list(dir)) {
                String name = path.getFileName().toString();
                long limit;
                if (name.endsWith(".sst") || name.equals(FileNames.CURRENT)) {
                    limit = fs.size(path);
                } else if (name.endsWith(".wal")) {
                    limit = provenSynced(fs, path, FileKind.WAL);
                } else if (name.startsWith("MANIFEST-")) {
                    limit = provenSynced(fs, path, FileKind.MANIFEST);
                } else {
                    continue;
                }
                if (limit > 0) {
                    candidates.add(path);
                    limits.add(limit);
                }
            }
            int choice = random.nextInt(candidates.size());
            Path target = candidates.get(choice);
            int limit = (int) (long) limits.get(choice);
            String targetName = target.getFileName().toString();
            byKind[targetName.endsWith(".sst") ? 0 : targetName.equals(FileNames.CURRENT) ? 2 : 1]++;

            int offset = random.nextInt(limit);
            int length = Math.min(limit - offset, switch (random.nextInt(4)) {
                case 0 -> 1;
                case 1 -> 1 + random.nextInt(64);
                default -> 1 + random.nextInt(4096);
            });
            byte[] before = fs.peek(target, offset, length);
            byte[] after = before.clone();
            switch (random.nextInt(4)) {
                case 0 -> after[random.nextInt(length)] ^= (byte) (1 << random.nextInt(8)); // bit flip
                case 1 -> Arrays.fill(after, (byte) 0);                                     // zeroed range
                case 2 -> random.nextBytes(after);                                          // garbage
                default -> {
                    // A block that was written to the wrong place: bytes of another table.
                    Path source = candidates.get(random.nextInt(candidates.size()));
                    int sourceSize = (int) fs.size(source);
                    int n = Math.min(length, sourceSize);
                    byte[] foreign = fs.peek(source, random.nextInt(sourceSize - n + 1), n);
                    System.arraycopy(foreign, 0, after, 0, n);
                }
            }
            if (Arrays.equals(before, after)) {
                return; // the damage happened to change nothing
            }
            fs.overwrite(target, offset, after);
            String what = targetName + ", " + length + " bytes at " + offset;

            boolean detected = false;
            LsmEngine engine = null;
            try {
                engine = LsmEngine.open(fs, dir, smallOptions());
            } catch (CorruptionException | UnsupportedFormatException e) {
                detected = true;
            }
            if (engine != null) {
                // Point reads: the right answer or a corruption error, never anything else.
                for (int i = 0; i < 60; i++) {
                    byte[] key = b("key-" + i);
                    try {
                        if (!Arrays.equals(model.get(key), engine.get(key))) {
                            fail("wrong data for key-" + i + " after damage to " + what);
                        }
                    } catch (CorruptionException e) {
                        detected = true;
                    }
                }
                // A full scan reads every data block.
                try (ScanIterator scan = engine.scan(null, null)) {
                    Iterator<Map.Entry<byte[], byte[]>> expected = model.entrySet().iterator();
                    for (; scan.valid(); scan.next()) {
                        Map.Entry<byte[], byte[]> entry = expected.hasNext() ? expected.next() : null;
                        if (entry == null || !Arrays.equals(entry.getKey(), scan.key())
                                || !Arrays.equals(entry.getValue(), scan.value())) {
                            fail("wrong data in a scan after damage to " + what);
                        }
                    }
                    if (expected.hasNext()) {
                        fail("a scan ended early without an error after damage to " + what);
                    }
                } catch (CorruptionException e) {
                    detected = true;
                }
                assertEquals(detected ? 1 : 0, Math.min(1, engine.damagedFileCount()),
                        "damage found by reads is counted: " + what);
                engine.close();
            }
            assertTrue(detected, "damage went unnoticed: " + what);
            assertEquals(0, fs.openHandleCount());
        });
        assertTrue(byKind[0] > 50 && byKind[1] > 20 && byKind[2] > 3,
                "damage per kind of file: " + Arrays.toString(byKind));
    }

    @Test
    void theListenerIsToldAboutDamageAndTheTableIsLeftAlone() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        List<CorruptionException> reports = new CopyOnWriteArrayList<>();
        RecordingMetrics metrics = new RecordingMetrics();
        EngineOptions options = smallOptions().memTableSize(1 << 20)
                .damageListener(reports::add).metrics(metrics);
        Path damaged;
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            for (int i = 0; i < 100; i++) {
                engine.write(new WriteBatch().put(b(String.format("key-%03d", i)), new byte[100]), false);
            }
            engine.flush();
            damaged = fs.list(dir).stream()
                    .filter(p -> p.getFileName().toString().endsWith(".sst")).findFirst().orElseThrow();
            fs.overwrite(damaged, 5000, new byte[] {1, 2, 3, 4, 5, 6, 7, 8});

            assertEquals(100, engine.get(b("key-000")).length, "blocks without damage still answer");
            assertTrue(reports.isEmpty());
            assertThrows(CorruptionException.class, () -> {
                for (int i = 0; i < 100; i++) {
                    engine.get(b(String.format("key-%03d", i)));
                }
            });
            assertEquals(1, reports.size());
            assertTrue(reports.get(0).file().endsWith(damaged.getFileName().toString()));
            assertTrue(reports.get(0).offset() > 0 && reports.get(0).offset() <= 5000);
            assertEquals(1, engine.damagedFileCount());
            assertEquals(1, metrics.counter("damage.files"));

            // A second table makes level 0 due for compaction; the damaged one is not touched.
            engine.put(b("other"), b("1"));
            engine.flush();
            assertFalse(engine.compact());
            assertEquals(2, engine.tableCount(0));
        }
    }

    @Test
    void aListenerThatFailsDoesNotHideTheDamage() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        EngineOptions options = smallOptions().damageListener(damage -> {
            throw new IllegalStateException("listener failure");
        });
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            engine.put(b("a"), new byte[50]);
            engine.flush();
            Path table = fs.list(dir).stream()
                    .filter(p -> p.getFileName().toString().endsWith(".sst")).findFirst().orElseThrow();
            fs.overwrite(table, 3, new byte[] {9, 9, 9});
            assertThrows(CorruptionException.class, () -> engine.get(b("a")));
        }
    }

    // ---- scrubbing -------------------------------------------------------------------------

    @Test
    void aScrubFindsDamageInBlocksThatNobodyReads() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        List<CorruptionException> reports = new CopyOnWriteArrayList<>();
        RecordingMetrics metrics = new RecordingMetrics();
        EngineOptions options = smallOptions().memTableSize(1 << 20).level0CompactionTrigger(100)
                .level0StallTrigger(100).damageListener(reports::add).metrics(metrics);
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            for (int table = 0; table < 5; table++) {
                for (int i = 0; i < 50; i++) {
                    engine.write(new WriteBatch().put(b("t" + table + "-" + i), new byte[100]), false);
                }
                engine.flush();
            }
            assertEquals(0, engine.scrub(), "an intact engine");
            assertTrue(metrics.counter("scrub.bytes") > 25_000);
            assertEquals(1, metrics.counter("scrub.passes"));

            List<Path> tables = fs.list(dir).stream()
                    .filter(p -> p.getFileName().toString().endsWith(".sst")).toList();
            fs.overwrite(tables.get(1), 2000, new byte[] {7});
            fs.overwrite(tables.get(3), fs.size(tables.get(3)) - 30, new byte[] {7}); // footer

            assertEquals(2, engine.scrub());
            assertEquals(2, reports.size());
            assertEquals(2, engine.damagedFileCount());
            assertEquals(2, engine.scrub(), "the damage stays until the replica is repaired");
            assertEquals(2, engine.damagedFileCount());
            assertEquals(0, engine.blockCache().pinnedCount());
            // Data outside the damaged blocks is still served.
            assertEquals(100, engine.get(b("t0-7")).length);
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void theBackgroundThreadScrubsPeriodically() throws Exception {
        MemoryFileSystem fs = new MemoryFileSystem();
        List<CorruptionException> reports = new CopyOnWriteArrayList<>();
        EngineOptions options = new EngineOptions().reserveSize(0).minFreeSpace(0)
                .blockCacheCapacity(64 * 1024).scrubPeriodMillis(20).damageListener(reports::add);
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            engine.put(b("a"), new byte[500]);
            engine.flush();
            Path table = fs.list(dir).stream()
                    .filter(p -> p.getFileName().toString().endsWith(".sst")).findFirst().orElseThrow();
            fs.overwrite(table, 10, new byte[] {1, 2, 3});
            long deadline = System.nanoTime() + 10_000_000_000L;
            while (reports.isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertFalse(reports.isEmpty(), "the periodic scrub did not report the damage");
        }
    }

    // ---- full disk -------------------------------------------------------------------------

    /** Returns the bytes in use, measured through the free space under a known capacity. */
    private long usedBytes(MemoryFileSystem fs) throws IOException {
        fs.setCapacity(1L << 40);
        long used = (1L << 40) - fs.freeSpace(dir);
        fs.setCapacity(Long.MAX_VALUE);
        return used;
    }

    @Test
    void writesAreRejectedWhenFreeSpaceIsLowAndResumeWhenItIsBack() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        RecordingMetrics metrics = new RecordingMetrics();
        EngineOptions options = smallOptions().minFreeSpace(10_000).metrics(metrics);
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            engine.put(b("a"), b("1"));
            fs.setCapacity(usedBytes(fs) + 9_999);
            DiskFullException e = assertThrows(DiskFullException.class, () -> engine.put(b("b"), b("2")));
            assertTrue(e.getMessage().contains("9999 bytes are free"), e.getMessage());
            assertEquals(1, metrics.counter("write.rejected.disk.full"));
            assertEquals("1", s(engine.get(b("a"))), "reads keep working");
            assertNull(engine.get(b("b")));
            engine.flush(); // flushes are not held back; they are how space is freed
            engine.compact();

            fs.setCapacity(Long.MAX_VALUE);
            engine.put(b("b"), b("2"));
            assertEquals("2", s(engine.get(b("b"))));
            assertEquals(2, engine.lastSequence());
        }
    }

    @Test
    void aWriteThatDoesNotFitIntoTheLogFailsAloneAndIsNeverReplayed() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        EngineOptions options = smallOptions().memTableSize(1 << 20);
        LsmEngine engine = LsmEngine.open(fs, dir, options);
        engine.put(b("before"), b("1"));
        long used = usedBytes(fs);
        fs.setCapacity(used + 600);

        // The first batch fits, the second does not; a third never gets written.
        engine.write(new WriteBatch().put(b("fits"), new byte[300]), true);
        DiskFullException e = assertThrows(DiskFullException.class,
                () -> engine.write(new WriteBatch().put(b("too-big-1"), new byte[200])
                        .put(b("too-big-2"), new byte[200]), true));
        assertTrue(e.getMessage().contains("did not fit"), e.getMessage());
        assertNull(engine.get(b("too-big-1")), "no part of the failed batch is visible");
        assertEquals(2, engine.lastSequence());
        assertEquals("1", s(engine.get(b("before"))));
        assertEquals(300, engine.get(b("fits")).length);

        // With space again the engine goes on, and the sequence numbers continue.
        fs.setCapacity(Long.MAX_VALUE);
        engine.put(b("after"), b("2"));
        assertEquals(3, engine.lastSequence());
        engine.close();
        assertEquals(0, fs.openHandleCount());

        try (LsmEngine reopened = LsmEngine.open(fs, dir, options)) {
            assertEquals(3, reopened.lastSequence());
            assertEquals("1", s(reopened.get(b("before"))));
            assertEquals(300, reopened.get(b("fits")).length);
            assertNull(reopened.get(b("too-big-1")));
            assertNull(reopened.get(b("too-big-2")));
            assertEquals("2", s(reopened.get(b("after"))));
        }
    }

    @Test
    void aCrashAfterAFailedLogWriteLosesNothingAcknowledged() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        EngineOptions options = smallOptions().memTableSize(1 << 20);
        LsmEngine engine = LsmEngine.open(fs, dir, options);
        engine.put(b("before"), b("1"));
        fs.setCapacity(usedBytes(fs) + 400);
        assertThrows(DiskFullException.class, () -> engine.put(b("too-big"), new byte[2000]));
        fs.crash(new Random(1));
        fs.setCapacity(Long.MAX_VALUE);
        try (LsmEngine reopened = LsmEngine.open(fs, dir, options)) {
            assertEquals("1", s(reopened.get(b("before"))));
            assertNull(reopened.get(b("too-big")));
            assertEquals(1, reopened.lastSequence());
        }
    }

    @Test
    void theReserveIsGivenUpForAFlushAndComesBackLater() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        RecordingMetrics metrics = new RecordingMetrics();
        EngineOptions options = smallOptions().memTableSize(1 << 20).reserveSize(30_000).metrics(metrics);
        Path reserve = dir.resolve(FileNames.RESERVE);
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            assertEquals(30_000, fs.size(reserve));
            for (int i = 0; i < 10; i++) {
                engine.put(b("key-" + i), new byte[1000]);
            }
            // Room for the next log, but not for the table of the flush.
            fs.setCapacity(usedBytes(fs) + 2_000);
            engine.flush();
            assertEquals(1, metrics.counter("reserve.released"), "the reserve made room for the flush");
            assertEquals(1, engine.tableCount(0));
            assertEquals(1000, engine.get(b("key-3")).length);

            // The flush deleted the log, so there is room again and the reserve is back.
            fs.setCapacity(Long.MAX_VALUE);
            engine.put(b("more"), b("1"));
            engine.flush();
            assertEquals(30_000, fs.size(reserve));
        }
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            for (int i = 0; i < 10; i++) {
                assertEquals(1000, engine.get(b("key-" + i)).length);
            }
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void aFlushWithoutSpaceFailsWithoutHarmAndSucceedsLater() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        EngineOptions options = smallOptions().memTableSize(1 << 20);
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            for (int i = 0; i < 10; i++) {
                engine.put(b("key-" + i), new byte[1000]);
            }
            fs.setCapacity(usedBytes(fs) + 2_000);
            DiskFullException e = assertThrows(DiskFullException.class, engine::flush);
            assertTrue(e.getMessage().contains("flush"), e.getMessage());
            assertEquals(0, engine.tableCount(0));
            assertEquals(1, engine.waitingMemTableCount());
            assertEquals(1000, engine.get(b("key-7")).length, "the data is still in memory and in its log");
            assertEquals(0, fs.list(dir).stream()
                    .filter(p -> p.getFileName().toString().endsWith(".sst")).count(), "no half-written table");

            fs.setCapacity(Long.MAX_VALUE);
            engine.put(b("after"), b("1")); // the engine has not failed
            engine.flush();
            assertEquals(0, engine.waitingMemTableCount());
            assertEquals(2, engine.tableCount(0));
        }
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            assertEquals(11, engine.lastSequence());
            assertEquals(1000, engine.get(b("key-7")).length);
            assertEquals("1", s(engine.get(b("after"))));
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void aReserveLeftUnfinishedByACrashIsCreatedAgain() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        EngineOptions options = smallOptions().reserveSize(5_000);
        LsmEngine.open(fs, dir, options).close();
        Path reserve = dir.resolve(FileNames.RESERVE);
        fs.truncate(reserve, 1234);
        LsmEngine.open(fs, dir, options).close();
        assertEquals(5_000, fs.size(reserve));
        // Without a reserve in the options, an existing file is left alone.
        LsmEngine.open(fs, dir, smallOptions()).close();
        assertEquals(5_000, fs.size(reserve));
    }

    // ---- figures ---------------------------------------------------------------------------

    @Test
    void theEngineReportsItsFigures() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        RecordingMetrics metrics = new RecordingMetrics();
        EngineOptions options = smallOptions().memTableSize(1 << 20).metrics(metrics);
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            engine.put(b("a"), new byte[100]);
            engine.write(new WriteBatch().put(b("b"), new byte[100]).put(b("c"), new byte[100]), false);
            assertEquals(2, metrics.counter("write.groups"));
            assertEquals(2, metrics.counter("write.batches"));
            assertTrue(metrics.counter("write.bytes") > 300);
            assertEquals(1, metrics.durations.get("wal.sync").get(), "only the first write synced");

            engine.flush();
            assertEquals(1, metrics.counter("flush.count"));
            assertTrue(metrics.counter("flush.bytes") > 300);
            assertEquals(1L, metrics.gauges.get("level.0.files"));
            assertEquals(0L, metrics.gauges.get("memtables.waiting"));

            engine.put(b("d"), new byte[100]);
            engine.flush();
            assertTrue(engine.compact());
            assertEquals(1, metrics.counter("compaction.count"));
            assertTrue(metrics.counter("compaction.bytes") > 400);
            assertEquals(0L, metrics.gauges.get("level.0.files"));
            assertEquals(1L, metrics.gauges.get("level.1.files"));
            assertTrue(metrics.gauges.get("level.1.bytes") > 400);

            try (Snapshot snapshot = engine.snapshot()) {
                engine.get(b("a"));
                engine.get(b("a"));
                engine.reportGauges();
                assertEquals(1L, metrics.gauges.get("snapshots.live"));
                assertTrue(metrics.gauges.get("cache.hits") >= 1);
                assertTrue(metrics.gauges.get("cache.misses") >= 1);
                assertTrue(metrics.gauges.get("table.metadata.bytes") > 0);
                assertTrue(metrics.gauges.get("disk.free.bytes") > 0);
                assertEquals(0L, metrics.gauges.get("damage.files.current"));
            }
            engine.reportGauges();
            assertEquals(0L, metrics.gauges.get("snapshots.live"));
            assertEquals(0L, metrics.gauges.get("snapshots.oldest.age.millis"));
        }
    }
}
