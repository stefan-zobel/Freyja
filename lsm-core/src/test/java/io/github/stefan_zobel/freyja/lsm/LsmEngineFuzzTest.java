package io.github.stefan_zobel.freyja.lsm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.common.ByteComparator;
import io.github.stefan_zobel.freyja.common.testing.Seeds;
import io.github.stefan_zobel.freyja.lsm.fs.MemoryFileSystem;

/**
 * Differential fuzzing of the engine against a sorted-map model
 * (docs/spec/verification.md, section 2.1), for the operations the engine has so far:
 * {@code put}, {@code delete}, batches, {@code get}, {@code scan}, {@code snapshot},
 * {@code release snapshot}, {@code flush}, {@code compact}, clean {@code reopen}, and crash. The MemTable
 * limit is tiny, so automatic flushes happen all the time.
 *
 * <p>A snapshot is a copy of the model. Every {@code get} and {@code scan}, on the live state
 * and on every open snapshot, must match the model.
 *
 * <p>After a crash the engine must hold a prefix of the write history that includes every
 * synced write. The test keeps the model state after each unsynced write and accepts any of
 * them; the engine then continues from the state it actually recovered.
 */
class LsmEngineFuzzTest {

    private static final int OPERATIONS_PER_SEED = 4000;
    private static final int MAX_SNAPSHOTS = 4;
    private static final int MAX_CHECKPOINTS = 3;

    /** An engine snapshot together with the model state it must show. */
    private record ModelSnapshot(Snapshot snapshot, TreeMap<byte[], byte[]> model) {
    }

    /** The model: user key to value, in unsigned byte order. */
    private static TreeMap<byte[], byte[]> newModel() {
        return new TreeMap<>(ByteComparator.UNSIGNED);
    }

    private static byte[] randomKey(Random random, byte[][] universe) {
        return universe[random.nextInt(universe.length)];
    }

    private static byte[] randomValue(Random random) {
        int length = switch (random.nextInt(20)) {
            case 0 -> 0;
            case 1 -> 30_000 + random.nextInt(40_000); // spans several log blocks
            default -> random.nextInt(64);
        };
        byte[] value = new byte[length];
        random.nextBytes(value);
        return value;
    }

    private static String hex(byte[] bytes) {
        return bytes == null ? "null" : HexFormat.of().formatHex(bytes);
    }

    /** Compares every key of the universe, read at {@code snapshot} or live if it is null. */
    private static boolean matches(LsmEngine engine, Snapshot snapshot, TreeMap<byte[], byte[]> model,
                                   byte[][] universe) throws IOException {
        for (byte[] key : universe) {
            byte[] actual = (snapshot == null) ? engine.get(key) : engine.get(key, snapshot);
            if (!Arrays.equals(model.get(key), actual)) {
                return false;
            }
        }
        return true;
    }

    /** Scans a range and compares it with the same range of the model, entry by entry. */
    private static void checkScan(LsmEngine engine, Snapshot snapshot, TreeMap<byte[], byte[]> model,
                                  byte[] start, byte[] end, String when) throws IOException {
        NavigableMap<byte[], byte[]> expected = model;
        if (start != null && end != null && ByteComparator.compare(start, end) >= 0) {
            expected = newModel(); // an empty or inverted range
        } else {
            if (start != null) {
                expected = expected.tailMap(start, true);
            }
            if (end != null) {
                expected = expected.headMap(end, false);
            }
        }
        Iterator<Map.Entry<byte[], byte[]>> model1 = expected.entrySet().iterator();
        try (ScanIterator scan = (snapshot == null)
                ? engine.scan(start, end) : engine.scan(start, end, snapshot)) {
            int index = 0;
            for (; scan.valid(); scan.next(), index++) {
                if (!model1.hasNext()) {
                    fail("scan [" + hex(start) + ", " + hex(end) + ") returns the extra key "
                            + hex(scan.key()) + " " + when);
                }
                Map.Entry<byte[], byte[]> entry = model1.next();
                if (!Arrays.equals(entry.getKey(), scan.key()) || !Arrays.equals(entry.getValue(), scan.value())) {
                    fail("scan [" + hex(start) + ", " + hex(end) + ") differs at entry " + index
                            + ": expected key " + hex(entry.getKey()) + ", got " + hex(scan.key()) + " " + when);
                }
            }
            if (model1.hasNext()) {
                fail("scan [" + hex(start) + ", " + hex(end) + ") ends before key "
                        + hex(model1.next().getKey()) + " " + when);
            }
        }
    }

    /** Returns a range bound: a key of the universe, a key near one, or no bound. */
    private static byte[] randomBound(Random random, byte[][] universe) {
        switch (random.nextInt(5)) {
            case 0:
                return null;
            case 1: {
                byte[] key = randomKey(random, universe).clone();
                key[random.nextInt(key.length)] ^= (byte) (1 << random.nextInt(8));
                return key;
            }
            case 2: {
                byte[] key = randomKey(random, universe);
                return Arrays.copyOf(key, key.length + 1); // just after the key
            }
            default:
                return randomKey(random, universe);
        }
    }

    private static void closeSnapshots(List<ModelSnapshot> snapshots) {
        for (ModelSnapshot s : snapshots) {
            s.snapshot().close();
        }
        snapshots.clear();
    }

    @Test
    void engineMatchesTheModelAcrossReopensAndCrashes() {
        Seeds.forEach(LsmEngineFuzzTest.class, "engine-fuzz", 40, seed -> {
            Random random = new Random(seed);
            MemoryFileSystem fs = new MemoryFileSystem();
            Path dir = Path.of("fuzz-" + seed).toAbsolutePath();
            EngineOptions options = new EngineOptions().deterministic(true)
                    .memTableSize(1024 + random.nextInt(64 * 1024))
                    .blockCacheCapacity(random.nextInt(4) == 0 ? 0 : 64 * 1024 * (1 + random.nextInt(8)))
                    // Small limits, so that data moves through several levels.
                    .levelCount(2 + random.nextInt(4))
                    .level0CompactionTrigger(1 + random.nextInt(4))
                    .level1TargetSize(4096 + random.nextInt(64 * 1024))
                    .targetFileSize(2048 + random.nextInt(32 * 1024))
                    // Tight budgets, so that index blocks are reloaded and the manifest is
                    // replaced many times.
                    .tableMetadataBudget(random.nextInt(3) == 0 ? random.nextInt(600) : 1 << 20)
                    .maxManifestSize(200 + random.nextInt(4000));

            // A small key universe makes overwrites and deletes of existing keys frequent.
            byte[][] universe = new byte[40 + random.nextInt(200)][];
            TreeMap<byte[], byte[]> distinct = newModel();
            while (distinct.size() < universe.length) {
                byte[] key = new byte[1 + random.nextInt(random.nextInt(8) == 0 ? 300 : 12)];
                random.nextBytes(key);
                distinct.put(key, key);
            }
            distinct.keySet().toArray(universe);

            TreeMap<byte[], byte[]> model = newModel();
            // Model states after each write since the last sync; index 0 is the synced state.
            List<TreeMap<byte[], byte[]>> sinceSync = new ArrayList<>();
            sinceSync.add(newModel());
            long expectedSequence = 0;
            List<Long> sequenceSinceSync = new ArrayList<>();
            sequenceSinceSync.add(0L);
            List<ModelSnapshot> snapshots = new ArrayList<>();
            int checkpoints = 0;

            LsmEngine engine = LsmEngine.open(fs, dir, options);
            for (int op = 0; op < OPERATIONS_PER_SEED; op++) {
                String when = "at operation " + op;
                int choice = random.nextInt(100);
                if (choice < 50) {
                    // write: a single put or delete, or a batch
                    WriteBatch batch = new WriteBatch();
                    int entries = random.nextInt(6) == 0 ? 1 + random.nextInt(12) : 1;
                    for (int i = 0; i < entries; i++) {
                        byte[] key = randomKey(random, universe);
                        if (random.nextInt(4) == 0) {
                            batch.delete(key);
                            model.remove(key);
                        } else {
                            byte[] value = randomValue(random);
                            batch.put(key, value);
                            model.put(key, value);
                        }
                    }
                    boolean sync = random.nextInt(3) != 0;
                    engine.write(batch, sync);
                    expectedSequence += entries;
                    if (sync) {
                        sinceSync.clear();
                        sequenceSinceSync.clear();
                    }
                    sinceSync.add(new TreeMap<>(model));
                    sequenceSinceSync.add(expectedSequence);
                    assertEquals(expectedSequence, engine.lastSequence());
                } else if (choice < 80) {
                    // get: live, or at a snapshot
                    byte[] key = randomKey(random, universe);
                    if (!snapshots.isEmpty() && random.nextInt(3) == 0) {
                        ModelSnapshot s = snapshots.get(random.nextInt(snapshots.size()));
                        if (!Arrays.equals(s.model().get(key), engine.get(key, s.snapshot()))) {
                            fail("get at a snapshot differs from the model " + when);
                        }
                    } else if (!Arrays.equals(model.get(key), engine.get(key))) {
                        fail("get differs from the model " + when);
                    }
                } else if (choice < 88) {
                    // scan: live, or at a snapshot
                    byte[] start = randomBound(random, universe);
                    byte[] end = randomBound(random, universe);
                    if (!snapshots.isEmpty() && random.nextInt(3) == 0) {
                        ModelSnapshot s = snapshots.get(random.nextInt(snapshots.size()));
                        checkScan(engine, s.snapshot(), s.model(), start, end, "at a snapshot " + when);
                    } else {
                        checkScan(engine, null, model, start, end, when);
                    }
                } else if (choice < 92) {
                    // take or release a snapshot
                    if (snapshots.size() < MAX_SNAPSHOTS && random.nextBoolean()) {
                        snapshots.add(new ModelSnapshot(engine.snapshot(), new TreeMap<>(model)));
                    } else if (!snapshots.isEmpty()) {
                        snapshots.remove(random.nextInt(snapshots.size())).snapshot().close();
                    }
                    assertEquals(snapshots.size(), engine.liveSnapshotCount());
                } else if (choice < 94) {
                    // compact: changes where the data lives, never what a reader sees
                    engine.compact();
                } else if (choice < 96) {
                    // flush or checkpoint: everything written so far becomes durable
                    if (choice == 95 && checkpoints < MAX_CHECKPOINTS) {
                        Path target = Path.of("fuzz-" + seed + "-checkpoint-" + checkpoints++).toAbsolutePath();
                        engine.checkpoint(target);
                        // The copy is a complete engine that shows exactly the current state.
                        try (LsmEngine copy = LsmEngine.open(fs, target, options)) {
                            if (!matches(copy, null, model, universe)) {
                                fail("a checkpoint differs from the model " + when);
                            }
                            checkScan(copy, null, model, null, null, "in a checkpoint " + when);
                            assertEquals(expectedSequence, copy.lastSequence());
                        }
                    } else {
                        engine.flush();
                    }
                    sinceSync.clear();
                    sequenceSinceSync.clear();
                    sinceSync.add(new TreeMap<>(model));
                    sequenceSinceSync.add(expectedSequence);
                } else if (choice < 98) {
                    // clean reopen: everything survives; snapshots end with the engine
                    for (ModelSnapshot s : snapshots) {
                        if (!matches(engine, s.snapshot(), s.model(), universe)) {
                            fail("a snapshot differs from the model before reopen " + when);
                        }
                    }
                    closeSnapshots(snapshots);
                    engine.close();
                    assertEquals(0, fs.openHandleCount());
                    engine = LsmEngine.open(fs, dir, options);
                    if (!matches(engine, null, model, universe)) {
                        fail("state differs from the model after reopen " + when);
                    }
                    checkScan(engine, null, model, null, null, "after reopen " + when);
                    assertEquals(expectedSequence, engine.lastSequence());
                    sinceSync.clear();
                    sequenceSinceSync.clear();
                    sinceSync.add(new TreeMap<>(model));
                    sequenceSinceSync.add(expectedSequence);
                } else {
                    // crash: a prefix of the history that includes every synced write
                    snapshots.clear(); // they die with the process
                    fs.crash();
                    engine = LsmEngine.open(fs, dir, options);
                    int recovered = -1;
                    for (int i = 0; i < sinceSync.size(); i++) {
                        if (sequenceSinceSync.get(i) == engine.lastSequence()
                                && matches(engine, null, sinceSync.get(i), universe)) {
                            recovered = i;
                            break;
                        }
                    }
                    if (recovered < 0) {
                        fail("state after the crash " + when
                                + " is no prefix of the write history that includes all synced writes");
                    }
                    model = new TreeMap<>(sinceSync.get(recovered));
                    expectedSequence = sequenceSinceSync.get(recovered);
                    checkScan(engine, null, model, null, null, "after the crash " + when);
                    sinceSync.clear();
                    sequenceSinceSync.clear();
                    sinceSync.add(new TreeMap<>(model));
                    sequenceSinceSync.add(expectedSequence);
                }
            }
            if (!matches(engine, null, model, universe)) {
                fail("state differs from the model at the end");
            }
            checkScan(engine, null, model, null, null, "at the end");
            for (ModelSnapshot s : snapshots) {
                checkScan(engine, s.snapshot(), s.model(), null, null, "at a snapshot at the end");
            }
            closeSnapshots(snapshots);
            assertEquals(0, engine.openScanCount());
            engine.close();
            assertEquals(0, fs.openHandleCount());
        });
    }
}
