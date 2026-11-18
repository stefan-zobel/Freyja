package io.github.stefan_zobel.freyja.lsm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.common.ByteComparator;
import io.github.stefan_zobel.freyja.common.testing.Seeds;
import io.github.stefan_zobel.freyja.lsm.fs.FaultyFileSystem;
import io.github.stefan_zobel.freyja.lsm.fs.FaultyFileSystem.SimulatedCrash;
import io.github.stefan_zobel.freyja.lsm.fs.MemoryFileSystem;
import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;

/**
 * Crash tests (docs/spec/verification.md, section 2.2): a workload is run again and again,
 * and each time the process "dies" at another file-system operation, with a random outcome
 * for everything that was not synced. After every crash the engine must open, must hold
 * every acknowledged write and no partial batch, and must accept new writes.
 */
class LsmEngineCrashTest {

    private static final int STEPS = 70;
    private static final Path DIR = Path.of("crash-test", "db").toAbsolutePath();

    private static EngineOptions options() {
        // Tiny limits: flushes, compactions through several levels, and manifest
        // replacements all happen within a short workload.
        return new EngineOptions().reserveSize(0).minFreeSpace(0)
                .deterministic(true)
                .blockCacheCapacity(64 * 1024)
                .memTableSize(1500)
                .level0CompactionTrigger(2)
                .level1TargetSize(5000)
                .targetFileSize(1500)
                .levelCount(4)
                .maxManifestSize(700)
                .tableMetadataBudget(300);
    }

    /** What the workload did, as far as the test needs it to judge a recovered state. */
    private static final class History {
        /** The model after each completed write; index 0 is the empty state. */
        final List<TreeMap<byte[], byte[]>> states = new ArrayList<>();
        final List<Long> sequences = new ArrayList<>();
        /** Index of the newest state that is known to be durable. */
        int durable;
        /** The state a write would produce that had started but not returned, or null. */
        TreeMap<byte[], byte[]> inFlight;
        long inFlightSequence;

        History() {
            states.add(new TreeMap<>(ByteComparator.UNSIGNED));
            sequences.add(0L);
        }

        TreeMap<byte[], byte[]> last() {
            return states.get(states.size() - 1);
        }

        void everythingIsDurable() {
            durable = states.size() - 1;
        }
    }

    private static byte[][] universe(Random random) {
        byte[][] keys = new byte[12][];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = ("key-" + (char) ('a' + i) + "-" + random.nextInt(1000)).getBytes();
        }
        return keys;
    }

    /** Runs the workload until it ends or the file system crashes. */
    private static void workload(StorageFileSystem fs, long seed, History history) throws IOException {
        Random random = new Random(seed);
        byte[][] keys = universe(random);
        EngineOptions options = options();
        LsmEngine engine = LsmEngine.open(fs, DIR, options);
        int checkpoints = 0;
        for (int step = 0; step < STEPS; step++) {
            int choice = random.nextInt(100);
            if (choice < 62) {
                WriteBatch batch = new WriteBatch();
                TreeMap<byte[], byte[]> next = new TreeMap<>(history.last());
                int entries = 1 + random.nextInt(3);
                for (int i = 0; i < entries; i++) {
                    byte[] key = keys[random.nextInt(keys.length)];
                    if (random.nextInt(4) == 0) {
                        batch.delete(key);
                        next.remove(key);
                    } else {
                        byte[] value = new byte[random.nextInt(40) == 0 ? 40_000 : random.nextInt(300)];
                        random.nextBytes(value);
                        batch.put(key, value);
                        next.put(key, value);
                    }
                }
                boolean sync = random.nextInt(3) != 0;
                long sequence = history.sequences.get(history.sequences.size() - 1) + entries;
                history.inFlight = next;
                history.inFlightSequence = sequence;
                engine.write(batch, sync);
                history.inFlight = null;
                history.states.add(next);
                history.sequences.add(sequence);
                if (sync) {
                    history.everythingIsDurable();
                }
            } else if (choice < 74) {
                engine.flush();
                history.everythingIsDurable();
            } else if (choice < 84) {
                engine.compact();
            } else if (choice < 88) {
                engine.checkpoint(DIR.resolveSibling("checkpoint-" + checkpoints++));
                history.everythingIsDurable();
            } else {
                engine.close();
                history.everythingIsDurable();
                engine = LsmEngine.open(fs, DIR, options);
            }
        }
        engine.close();
        history.everythingIsDurable();
    }

    /** Returns the index of the acceptable state that the engine shows, or -1. */
    private static int recoveredState(LsmEngine engine, History history, byte[][] keys) throws IOException {
        List<TreeMap<byte[], byte[]>> candidates = new ArrayList<>(
                history.states.subList(history.durable, history.states.size()));
        List<Long> sequences = new ArrayList<>(
                history.sequences.subList(history.durable, history.sequences.size()));
        if (history.inFlight != null) {
            candidates.add(history.inFlight);
            sequences.add(history.inFlightSequence);
        }
        for (int i = 0; i < candidates.size(); i++) {
            if (sequences.get(i) != engine.lastSequence()) {
                continue;
            }
            boolean same = true;
            for (byte[] key : keys) {
                if (!Arrays.equals(candidates.get(i).get(key), engine.get(key))) {
                    same = false;
                    break;
                }
            }
            if (same) {
                // A scan must agree with the point reads.
                int count = 0;
                try (ScanIterator scan = engine.scan(null, null)) {
                    for (; scan.valid(); scan.next()) {
                        if (!Arrays.equals(candidates.get(i).get(scan.key()), scan.value())) {
                            return -1;
                        }
                        count++;
                    }
                }
                return count == candidates.get(i).size() ? i : -1;
            }
        }
        return -1;
    }

    /** Opens the crashed directory and checks everything the fault model promises. */
    private static void verify(MemoryFileSystem memory, History history, long seed, String where)
            throws IOException {
        byte[][] keys = universe(new Random(seed));
        EngineOptions options = options();
        LsmEngine engine;
        try {
            engine = LsmEngine.open(memory, DIR, options);
        } catch (IOException e) {
            throw new AssertionError("the engine does not open after " + where, e);
        }
        if (recoveredState(engine, history, keys) < 0) {
            fail("after " + where + " the engine holds no state between the last durable write (number "
                    + history.durable + ") and the last write started; last sequence " + engine.lastSequence());
        }
        // The engine accepts new writes, and they survive a reopen.
        byte[] marker = "written-after-the-crash".getBytes();
        engine.put(marker, marker);
        engine.flush();
        engine.compact();
        engine.close();
        assertEquals(0, memory.openHandleCount(), "handles left open after " + where);
        try (LsmEngine again = LsmEngine.open(memory, DIR, options)) {
            assertTrue(Arrays.equals(marker, again.get(marker)), "a write after " + where + " was lost");
        }
    }

    @Test
    void aCrashAtEveryFileSystemOperationLosesNothingAcknowledged() {
        Seeds.forEach(LsmEngineCrashTest.class, "engine-crash", 3, seed -> {
            // A run without a crash counts the operations.
            FaultyFileSystem counting = new FaultyFileSystem(new MemoryFileSystem());
            workload(counting, seed, new History());
            long operations = counting.operationCount();
            assertTrue(operations > 300, "the workload is too small: " + operations + " operations");

            for (long crashAt = 0; crashAt < operations; crashAt++) {
                MemoryFileSystem memory = new MemoryFileSystem();
                FaultyFileSystem faulty = new FaultyFileSystem(memory);
                Random crashRandom = new Random(seed * 1_000_003 + crashAt);
                faulty.crashAt(crashAt, crashRandom, () -> memory.crash(crashRandom));
                History history = new History();
                try {
                    workload(faulty, seed, history);
                    fail("no crash at operation " + crashAt + " of " + operations);
                } catch (SimulatedCrash expected) {
                    // The process died here.
                }
                String where = "a crash at operation " + crashAt + " of " + operations;

                // Now and then the recovery itself crashes too, once or twice in a row.
                for (int again = 0; again < 2 && crashAt % 5 == 0; again++) {
                    FaultyFileSystem second = new FaultyFileSystem(memory);
                    second.crashAt(crashRandom.nextInt(25), crashRandom, () -> memory.crash(crashRandom));
                    try {
                        LsmEngine.open(second, DIR, options()).close();
                    } catch (SimulatedCrash expected) {
                        where += ", then a crash during recovery";
                    } catch (IOException e) {
                        throw new AssertionError("the engine does not open after " + where, e);
                    }
                }
                verify(memory, history, seed, where);
            }
        });
    }
}
