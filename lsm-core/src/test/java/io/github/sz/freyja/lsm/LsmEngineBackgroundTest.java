package io.github.sz.freyja.lsm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.lsm.fs.MemoryFileSystem;

/**
 * The engine in normal mode, where a background thread flushes and compacts, and the
 * backpressure that holds writes back when that thread is behind. Deterministic mode is
 * covered by the other engine tests.
 */
class LsmEngineBackgroundTest {

    private final MemoryFileSystem fs = new MemoryFileSystem();
    private final Path dir = Path.of("background-test").toAbsolutePath();
    private final EngineOptions options = new EngineOptions().reserveSize(0).minFreeSpace(0)
            .blockCacheCapacity(1 << 20)
            .memTableSize(8 * 1024)
            .level0CompactionTrigger(2)
            .level1TargetSize(32 * 1024)
            .targetFileSize(8 * 1024);

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String s(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    private static String key(int i) {
        return String.format("key-%05d", i);
    }

    /** Waits until the condition holds; fails after ten seconds. */
    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for: " + what);
            }
            Thread.sleep(2);
        }
    }

    private static boolean settled(LsmEngine engine, EngineOptions options) {
        return engine.waitingMemTableCount() == 0
                && engine.tableCount(0) < options.level0CompactionTrigger();
    }

    @Test
    void flushAndCompactionHappenWithoutBeingAsked() throws Exception {
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            for (int round = 0; round < 5; round++) {
                for (int i = 0; i < 400; i++) {
                    engine.write(new WriteBatch().put(b(key(i)), b("round-" + round + "-" + "x".repeat(50))), false);
                }
            }
            await("background work to settle", () -> settled(engine, options));
            assertTrue(engine.tableCount(1) + engine.tableCount(2) + engine.tableCount(3) > 0,
                    "data reached the levels below 0");
            assertEquals(null, engine.lastCompactionProblem());
            for (int i = 0; i < 400; i++) {
                assertTrue(s(engine.get(b(key(i)))).startsWith("round-4-"), key(i));
            }
        }
        assertEquals(0, fs.openHandleCount());
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            for (int i = 0; i < 400; i++) {
                assertTrue(s(engine.get(b(key(i)))).startsWith("round-4-"), key(i));
            }
            assertEquals(2000, engine.lastSequence());
        }
    }

    /**
     * A few keys are overwritten all the time, so that every compaction drops versions of
     * them. A read without a snapshot must still see at least what was written before it
     * began: it must not take its sequence number first and then read a state from which a
     * compaction has removed the version that number sees.
     */
    @Test
    void aReadWithoutASnapshotSeesWhatWasWrittenBeforeItWhileCompactionsDropVersions() throws Exception {
        int keys = 4;
        int rounds = 3_000;
        AtomicLongArray written = new AtomicLongArray(keys);
        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            List<Thread> readers = new ArrayList<>();
            for (int r = 0; r < 8; r++) {
                int reader = r;
                readers.add(new Thread(() -> {
                    try {
                        for (long i = reader; !stop.get(); i++) {
                            int index = (int) (i % keys);
                            long finished = written.get(index);
                            byte[] value;
                            String how;
                            if (reader % 3 == 0) {
                                how = "get";
                                value = engine.get(b(key(index)));
                            } else {
                                how = reader % 3 == 1 ? "scan" : "reverse scan";
                                try (ScanIterator scan = reader % 3 == 1
                                        ? engine.scan(b(key(index)), b(key(index + 1)))
                                        : engine.scanReverse(b(key(index)), b(key(index + 1)))) {
                                    value = scan.valid() ? scan.value() : null;
                                }
                            }
                            long counter = value == null ? 0 : Long.parseLong(s(value).split("-")[0]);
                            if (counter < finished) {
                                throw new AssertionError(how + " of key " + index + " returned " + counter
                                        + " after " + finished + " was written");
                            }
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    }
                }, "reader-" + r));
            }
            for (Thread thread : readers) {
                thread.start();
            }
            for (long round = 1; round <= rounds && failure.get() == null; round++) {
                for (int index = 0; index < keys; index++) {
                    engine.put(b(key(index)), b(round + "-" + "v".repeat(100)));
                    written.set(index, round);
                }
            }
            stop.set(true);
            for (Thread thread : readers) {
                thread.join();
            }
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void readersAndScansStayConsistentWhileBackgroundWorkRuns() throws Exception {
        int writers = 4;
        int keysPerWriter = 50;
        int rounds = 150;
        // The last counter value each writer has written for each of its keys.
        AtomicLongArray written = new AtomicLongArray(writers * keysPerWriter);
        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            List<Thread> threads = new ArrayList<>();
            for (int w = 0; w < writers; w++) {
                int writer = w;
                threads.add(new Thread(() -> {
                    try {
                        for (long round = 1; round <= rounds; round++) {
                            for (int k = 0; k < keysPerWriter; k++) {
                                int index = writer * keysPerWriter + k;
                                // Two keys in one batch always carry the same counter.
                                engine.write(new WriteBatch()
                                        .put(b("a-" + key(index)), b(round + "-" + "p".repeat(40)))
                                        .put(b("b-" + key(index)), b(round + "-" + "q".repeat(40))), false);
                                written.set(index, round);
                            }
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    }
                }, "writer-" + w));
            }
            for (int r = 0; r < 3; r++) {
                threads.add(new Thread(() -> {
                    long[] seen = new long[writers * keysPerWriter];
                    try {
                        while (!stop.get()) {
                            // Point reads: a counter never goes backward and is never
                            // older than what its writer had finished before the read.
                            for (int index = 0; index < seen.length && !stop.get(); index++) {
                                long finished = written.get(index);
                                byte[] value = engine.get(b("a-" + key(index)));
                                long counter = value == null ? 0 : Long.parseLong(s(value).split("-")[0]);
                                if (counter < finished || counter < seen[index]) {
                                    throw new AssertionError("key " + index + " went back to " + counter
                                            + " after " + Math.max(finished, seen[index]));
                                }
                                seen[index] = counter;
                            }
                            // A scan sees one moment: sorted keys, and both keys of a batch
                            // with the same counter.
                            try (Snapshot snapshot = engine.snapshot();
                                 ScanIterator scan = engine.scan(null, null, snapshot)) {
                                String previous = "";
                                int count = 0;
                                for (; scan.valid(); scan.next(), count++) {
                                    String k = s(scan.key());
                                    if (k.compareTo(previous) <= 0) {
                                        throw new AssertionError("scan out of order at " + k);
                                    }
                                    previous = k;
                                    if (k.startsWith("a-")) {
                                        String partner = s(engine.get(b("b-" + k.substring(2)), snapshot));
                                        String mine = s(scan.value()).split("-")[0];
                                        if (partner == null || !partner.split("-")[0].equals(mine)) {
                                            throw new AssertionError("batch seen in part at " + k);
                                        }
                                    }
                                }
                                if (count % 2 != 0) {
                                    throw new AssertionError("scan saw an odd number of keys: " + count);
                                }
                            }
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    }
                }, "reader-" + r));
            }
            for (Thread thread : threads) {
                thread.start();
            }
            for (int w = 0; w < writers; w++) {
                threads.get(w).join();
            }
            stop.set(true);
            for (Thread thread : threads) {
                thread.join();
            }
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }

            await("background work to settle", () -> settled(engine, options));
            assertEquals(2L * writers * keysPerWriter * rounds, engine.lastSequence());
            for (int index = 0; index < writers * keysPerWriter; index++) {
                assertTrue(s(engine.get(b("a-" + key(index)))).startsWith(rounds + "-"));
                assertTrue(s(engine.get(b("b-" + key(index)))).startsWith(rounds + "-"));
            }
            assertEquals(0, engine.openScanCount());
            assertEquals(0, engine.liveSnapshotCount());
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void aWriteThatStallsTooLongFailsWithoutHarmingTheEngine() throws Exception {
        HookedFileSystem hooked = new HookedFileSystem(fs);
        options.maxImmutableMemTables(1).writeStallTimeoutMillis(40);
        try (LsmEngine engine = LsmEngine.open(hooked, dir, options)) {
            hooked.syncDelayMillis = 300; // every flush now takes far longer than a write waits
            WriteStalledException stalled = null;
            int accepted = 0;
            for (int i = 0; i < 2000 && stalled == null; i++) {
                try {
                    engine.write(new WriteBatch().put(b(key(i)), new byte[200]), false);
                    accepted++;
                } catch (WriteStalledException e) {
                    stalled = e;
                }
            }
            assertNotNull(stalled, "a write should have stalled");
            assertTrue(stalled.getMessage().contains("wait for their flush"), stalled.getMessage());
            assertFalse(accepted == 0);

            // The engine has not failed: once the flush catches up, writes go on.
            hooked.syncDelayMillis = 0;
            await("the flush to catch up", () -> engine.waitingMemTableCount() == 0);
            engine.put(b("after"), b("ok"));
            assertEquals("ok", s(engine.get(b("after"))));
            for (int i = 0; i < accepted; i++) {
                assertEquals(200, engine.get(b(key(i))).length, key(i));
            }
            assertEquals(null, engine.get(b(key(accepted))), "the stalled write stored nothing");
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void closingWhileMemTablesWaitLosesNothing() throws Exception {
        HookedFileSystem hooked = new HookedFileSystem(fs);
        options.maxImmutableMemTables(4).writeStallTimeoutMillis(10_000);
        LsmEngine engine = LsmEngine.open(hooked, dir, options);
        hooked.syncDelayMillis = 30;
        for (int i = 0; i < 300; i++) {
            engine.write(new WriteBatch().put(b(key(i)), new byte[200]), false);
        }
        engine.close(); // some MemTables have not been flushed yet
        hooked.syncDelayMillis = 0;
        assertEquals(0, fs.openHandleCount());
        assertThrows(IOException.class, () -> engine.put(b("x"), b("y")));

        try (LsmEngine reopened = LsmEngine.open(fs, dir, options)) {
            assertEquals(300, reopened.lastSequence());
            for (int i = 0; i < 300; i++) {
                assertEquals(200, reopened.get(b(key(i))).length, key(i));
            }
        }
    }

    @Test
    void aCrashWhileMemTablesWaitLosesOnlyUnsyncedWrites() throws Exception {
        HookedFileSystem hooked = new HookedFileSystem(fs);
        options.maxImmutableMemTables(4).writeStallTimeoutMillis(10_000);
        LsmEngine engine = LsmEngine.open(hooked, dir, options);
        hooked.syncDelayMillis = 20;
        for (int i = 0; i < 200; i++) {
            engine.write(new WriteBatch().put(b(key(i)), new byte[200]), false);
        }
        engine.put(b("synced"), b("yes")); // everything before it is durable as well
        engine.write(new WriteBatch().put(b("unsynced"), b("no")), false);
        fs.crash();
        hooked.syncDelayMillis = 0;
        // The old instance is dead; its background thread runs into the crashed file system.
        try (LsmEngine reopened = LsmEngine.open(fs, dir, options)) {
            assertEquals(201, reopened.lastSequence());
            assertEquals("yes", s(reopened.get(b("synced"))));
            assertEquals(null, reopened.get(b("unsynced")));
            for (int i = 0; i < 200; i++) {
                assertEquals(200, reopened.get(b(key(i))).length, key(i));
            }
        }
    }

    @Test
    void inDeterministicModeTheCallerDrivesCompaction() throws IOException {
        options.deterministic(true).memTableSize(1 << 20);
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            assertFalse(engine.runBackgroundWork());
            engine.put(b("a"), b("1"));
            engine.flush();
            engine.put(b("b"), b("2"));
            engine.flush();
            assertEquals(2, engine.tableCount(0), "nothing happens on its own");
            assertTrue(engine.runBackgroundWork());
            assertEquals(0, engine.tableCount(0));
            assertEquals(1, engine.tableCount(1));
            assertFalse(engine.runBackgroundWork());
            assertEquals("1", s(engine.get(b("a"))));
        }
    }

    @Test
    void inDeterministicModeAWriteCompactsInsteadOfStalling() throws IOException {
        options.deterministic(true).memTableSize(2048).level0CompactionTrigger(2).level0StallTrigger(3);
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            for (int i = 0; i < 500; i++) {
                engine.write(new WriteBatch().put(b(key(i)), new byte[100]), false);
                assertTrue(engine.tableCount(0) <= 3, "level 0 holds " + engine.tableCount(0) + " files");
            }
            assertTrue(engine.tableCount(1) + engine.tableCount(2) > 0);
            for (int i = 0; i < 500; i++) {
                assertEquals(100, engine.get(b(key(i))).length);
            }
        }
    }

    @Test
    void aStallLimitBelowTheCompactionTriggerIsRejected() {
        options.level0CompactionTrigger(8).level0StallTrigger(4);
        assertThrows(IllegalArgumentException.class, () -> LsmEngine.open(fs, dir, options));
    }
}
