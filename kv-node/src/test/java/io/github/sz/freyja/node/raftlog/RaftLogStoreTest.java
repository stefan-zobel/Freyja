package io.github.sz.freyja.node.raftlog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.common.testing.Seeds;
import io.github.sz.freyja.lsm.UnsupportedFormatException;
import io.github.sz.freyja.lsm.fs.FaultyFileSystem;
import io.github.sz.freyja.lsm.fs.FaultyFileSystem.SimulatedCrash;
import io.github.sz.freyja.lsm.fs.MemoryFileSystem;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.ConfigChange;
import io.github.sz.freyja.raft.EntryType;
import io.github.sz.freyja.raft.LogEntry;

/** The Raft log store (docs/spec/node.md, section 3), also under crashes and damage. */
class RaftLogStoreTest {

    private static final Path DIR = Path.of("data", "raft");
    private static final ClusterConfig THREE = ClusterConfig.ofVoters(0, 1, 2);
    private static final int SEGMENT = 2048;

    private static RaftLogStore open(StorageFileSystem fs) throws IOException {
        return RaftLogStore.open(fs, DIR, SEGMENT, 0, THREE);
    }

    private static LogEntry entry(long index, long term, int size) {
        byte[] payload = new byte[size];
        Arrays.fill(payload, (byte) (index * 31 + term));
        return new LogEntry(index, term, EntryType.COMMAND, payload);
    }

    private static List<LogEntry> batch(long from, int count, long term, int size) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            entries.add(entry(from + i, term, size));
        }
        return entries;
    }

    private static String shape(LogEntry entry) {
        return entry.index() + "@" + entry.term() + ":" + entry.type() + ":" + Arrays.hashCode(entry.payload());
    }

    private static List<String> shapes(List<LogEntry> entries) {
        return entries.stream().map(RaftLogStoreTest::shape).toList();
    }

    /** Everything the store holds from {@code from} on. */
    private static List<String> content(RaftLogStore store, long from) {
        assertTrue(store.firstIndex() <= from, "the log starts at " + store.firstIndex());
        List<LogEntry> all = store.entries(from, store.lastIndex() + 1, Integer.MAX_VALUE);
        for (LogEntry entry : all) {
            assertEquals(entry.term(), store.termAt(entry.index()));
        }
        return shapes(all);
    }

    @Test
    void entriesSurviveAReopenAcrossSeveralSegments() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        List<LogEntry> model = new ArrayList<>();
        try (RaftLogStore store = open(fs)) {
            assertEquals(1, store.firstIndex());
            assertEquals(0, store.lastIndex());
            for (int i = 0; i < 40; i++) {
                List<LogEntry> entries = batch(model.size() + 1, 1 + i % 4, 1 + i / 10, 100 + i);
                store.append(entries);
                model.addAll(entries);
                store.sync();
            }
            assertTrue(store.segmentCount() > 5, "segments: " + store.segmentCount());
            assertEquals(shapes(model), content(store, 1));
            assertEquals(shapes(model.subList(9, 12)), shapes(store.entries(10, 13, Integer.MAX_VALUE)));
            assertEquals(1, store.entries(10, 13, 1).size(), "at least one entry, whatever the limit");
            assertEquals(2, store.entries(10, 13, model.get(9).size() + model.get(10).size()).size());
        }
        assertEquals(0, fs.openHandleCount());
        try (RaftLogStore store = open(fs)) {
            assertEquals(-1, store.firstDamagedIndex());
            assertEquals(model.size(), store.lastIndex());
            assertEquals(shapes(model), content(store, 1));
            assertThrows(IllegalArgumentException.class, () -> store.append(batch(model.size() + 2, 1, 9, 1)));
        }
    }

    @Test
    void aTruncatedSuffixIsGoneAndItsIndexesAreWrittenAgain() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        try (RaftLogStore store = open(fs)) {
            store.append(batch(1, 30, 1, 150));
            store.sync();
            store.truncateSuffix(11);
            assertEquals(10, store.lastIndex());
            store.truncateSuffix(50);
            store.append(batch(11, 5, 2, 150));
            store.sync();
            assertEquals(15, store.lastIndex());
            assertEquals(2, store.termAt(11));
            assertThrows(IllegalArgumentException.class, () -> store.truncateSuffix(0));
        }
        try (RaftLogStore store = open(fs)) {
            List<LogEntry> expected = new ArrayList<>(batch(1, 10, 1, 150));
            expected.addAll(batch(11, 5, 2, 150));
            assertEquals(shapes(expected), content(store, 1));
        }
    }

    @Test
    void theConfigurationAtTheEndOfTheLogIsKnownAfterAReopen() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        ClusterConfig grown = THREE.with(new ConfigChange.AddLearner(3));
        try (RaftLogStore store = open(fs)) {
            store.append(batch(1, 3, 1, 10));
            store.append(List.of(new LogEntry(4, 1, EntryType.CONFIG, grown.encode())));
            store.sync();
            assertEquals(grown, store.configAtEnd());
            store.truncateSuffix(4);
            assertEquals(THREE, store.configAtEnd());
            store.append(List.of(new LogEntry(4, 2, EntryType.CONFIG, grown.encode())));
            store.sync();
        }
        try (RaftLogStore store = open(fs)) {
            assertEquals(grown, store.configAtEnd());
            assertEquals(EntryType.CONFIG, store.entries(4, 5, 100).getFirst().type());
        }
    }

    @Test
    void aPrefixIsRemovedBySegments() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        try (RaftLogStore store = open(fs)) {
            for (int i = 0; i < 30; i++) {
                store.append(batch(store.lastIndex() + 1, 3, 1, 200));
                store.sync();
            }
            int before = store.segmentCount();
            store.truncatePrefix(45);
            assertTrue(store.segmentCount() < before);
            assertTrue(store.firstIndex() <= 46 && store.firstIndex() > 20, "first index " + store.firstIndex());
            assertEquals(shapes(batch(46, 45, 1, 200)), content(store, 46));
            store.truncatePrefix(90);
            assertEquals(1, store.segmentCount(), "the segment being written stays");
        }
        assertEquals(0, fs.openHandleCount());
        try (RaftLogStore store = RaftLogStore.open(fs, DIR, SEGMENT, 90, THREE)) {
            assertEquals(-1, store.firstDamagedIndex());
            assertEquals(90, store.lastIndex());
            store.append(batch(91, 2, 2, 5));
            store.sync();
            assertEquals(92, store.lastIndex());
        }
    }

    /** One operation of the model: the log content after it. */
    private record State(List<LogEntry> entries) {
    }

    @Test
    void afterACrashTheLogIsAStateItHadSinceItsLastSyncAndNeverDamaged() {
        Seeds.forEach(RaftLogStoreTest.class, "raft-log-crash", 60, seed -> {
            Random random = new Random(seed);
            MemoryFileSystem fs = new MemoryFileSystem();
            List<LogEntry> model = new ArrayList<>();
            long snapshot = 0;
            for (int round = 0; round < 12; round++) {
                // What the log may be after a crash: every state since the last sync.
                List<State> possible = new ArrayList<>();
                try (RaftLogStore store = RaftLogStore.open(fs, DIR, SEGMENT, snapshot, THREE)) {
                    assertEquals(-1, store.firstDamagedIndex(), "a crash is not damage");
                    assertEquals(model.size(), store.lastIndex());
                    assertEquals(shapes(model.subList((int) snapshot, model.size())), content(store, snapshot + 1));
                    possible.add(new State(new ArrayList<>(model)));
                    int operations = 1 + random.nextInt(25);
                    for (int i = 0; i < operations; i++) {
                        int choice = random.nextInt(10);
                        if (choice < 6) {
                            List<LogEntry> entries = batch(model.size() + 1, 1 + random.nextInt(4),
                                    1 + round, random.nextInt(400));
                            store.append(entries);
                            model.addAll(entries);
                        } else if (choice < 7 && model.size() > snapshot + 1) {
                            long from = snapshot + 2 + random.nextInt((int) (model.size() - snapshot - 1));
                            store.truncateSuffix(from);
                            while (model.size() >= from) {
                                model.removeLast();
                            }
                        } else if (choice < 9) {
                            store.sync();
                            possible.clear();
                        } else {
                            store.sync();
                            possible.clear();
                            snapshot += random.nextInt((int) (model.size() - snapshot) + 1);
                            store.truncatePrefix(snapshot);
                        }
                        possible.add(new State(new ArrayList<>(model)));
                    }
                    fs.crash(random);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
                // Find out which state the crash left.
                try (RaftLogStore store = RaftLogStore.open(fs, DIR, SEGMENT, snapshot, THREE)) {
                    List<String> recovered = content(store, snapshot + 1);
                    State left = null;
                    for (State state : possible) {
                        if (shapes(state.entries().subList((int) snapshot, state.entries().size())).equals(recovered)) {
                            left = state;
                        }
                    }
                    if (left == null) {
                        fail("round " + round + ": the log after the crash, " + store.firstIndex() + ".."
                                + store.lastIndex() + ", is none of the " + possible.size()
                                + " states since the last sync");
                    }
                    model.clear();
                    model.addAll(left.entries());
                }
            }
            assertEquals(0, fs.openHandleCount());
        });
    }

    @Test
    void aCrashAtEveryFileSystemOperationLeavesAUsableLog() throws IOException {
        // The operations of a workload with several segments, a truncation, and a prefix removal.
        FaultyFileSystem counting = new FaultyFileSystem(new MemoryFileSystem());
        workload(counting);
        long operations = counting.operationCount();
        assertTrue(operations > 40, "operations: " + operations);
        for (long at = 0; at < operations; at++) {
            MemoryFileSystem memory = new MemoryFileSystem();
            FaultyFileSystem faulty = new FaultyFileSystem(memory);
            Random random = new Random(at);
            faulty.crashAt(at, random, () -> memory.crash(random));
            try {
                workload(faulty);
                fail("no crash at operation " + at);
            } catch (SimulatedCrash expected) {
                // the node is gone
            }
            for (long snapshot : new long[] {0, 20}) {
                try (RaftLogStore store = RaftLogStore.open(memory, DIR, SEGMENT, snapshot, THREE)) {
                    if (store.firstDamagedIndex() != -1) {
                        // Only if the prefix up to index 20 was already removed.
                        assertEquals(0, snapshot, "damage after a crash at operation " + at);
                        continue;
                    }
                    long last = store.lastIndex();
                    for (LogEntry entry : store.entries(Math.max(store.firstIndex(), snapshot + 1), last + 1,
                            Integer.MAX_VALUE)) {
                        assertTrue(entry.term() == 1 || entry.term() == 2);
                    }
                    store.append(batch(last + 1, 2, 3, 10));
                    store.sync();
                    break;
                }
            }
            assertEquals(0, memory.openHandleCount());
        }
    }

    private static void workload(StorageFileSystem fs) throws IOException {
        try (RaftLogStore store = RaftLogStore.open(fs, DIR, 1024, 0, THREE)) {
            for (int i = 0; i < 8; i++) {
                store.append(batch(store.lastIndex() + 1, 3, 1, 150));
                store.sync();
            }
            store.truncateSuffix(22);
            store.append(batch(22, 4, 2, 150));
            store.sync();
            store.truncatePrefix(20);
            store.append(batch(26, 2, 2, 150));
            store.sync();
        }
    }

    private static List<Path> segmentFiles(MemoryFileSystem fs) throws IOException {
        return fs.list(DIR).stream().filter(p -> p.getFileName().toString().endsWith(".rlog")).sorted().toList();
    }

    @Test
    void damageIsReportedNotCutAwayAndTheDamagedSegmentsAreSetAside() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        try (RaftLogStore store = open(fs)) {
            for (int i = 0; i < 12; i++) {
                store.append(batch(store.lastIndex() + 1, 3, 1, 300));
                store.sync();
            }
        }
        List<Path> files = segmentFiles(fs);
        assertTrue(files.size() >= 5);
        Path victim = files.get(2);
        byte[] original = fs.peek(victim, 0, (int) fs.size(victim));
        byte[] flipped = {(byte) (original[200] ^ 0x10)};
        fs.overwrite(victim, 200, flipped);

        long vouched;
        try (RaftLogStore store = open(fs)) {
            vouched = store.lastIndex();
            assertEquals(vouched + 1, store.firstDamagedIndex());
            assertTrue(vouched > 0 && vouched < 36, "vouched for: " + vouched);
            assertEquals(files.size() - 2, store.damagedFiles().size(), "the damaged segment and all after it");
            assertEquals(shapes(batch(1, (int) vouched, 1, 300)), content(store, 1));
            assertThrows(IllegalStateException.class, () -> store.append(batch(vouched + 1, 1, 2, 1)));
            assertThrows(IllegalStateException.class, store::sync);
        }
        // Nothing was changed: the damage is found again.
        assertEquals(files, segmentFiles(fs));
        try (RaftLogStore store = open(fs)) {
            assertEquals(vouched + 1, store.firstDamagedIndex());
            store.quarantineDamaged();
            assertEquals(-1, store.firstDamagedIndex());
            store.append(batch(vouched + 1, 2, 2, 50));
            store.sync();
        }
        assertEquals(files.size() - 2, fs.list(DIR.resolve(RaftLogStore.DAMAGED_DIRECTORY)).size());
        assertEquals(original.length, fs.size(DIR.resolve(RaftLogStore.DAMAGED_DIRECTORY).resolve(victim.getFileName())),
                "set aside as it was");
        try (RaftLogStore store = open(fs)) {
            assertEquals(-1, store.firstDamagedIndex());
            assertEquals(vouched + 2, store.lastIndex());
            assertEquals(2, store.termAt(vouched + 1));
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void everyDamageOfSyncedSegmentsIsNoticed() {
        Seeds.forEach(RaftLogStoreTest.class, "raft-log-damage", 150, seed -> {
            Random random = new Random(seed);
            MemoryFileSystem fs = new MemoryFileSystem();
            try (RaftLogStore store = open(fs)) {
                for (int i = 0; i < 10; i++) {
                    store.append(batch(store.lastIndex() + 1, 2, 1, 250));
                    store.sync();
                }
            }
            List<Path> files = segmentFiles(fs);
            // The last file holds only a header; every other one was synced as a whole.
            Path victim = files.get(random.nextInt(files.size() - 1));
            int size = (int) fs.size(victim);
            int at = random.nextInt(size);
            int kind = random.nextInt(3);
            if (kind == 0) {
                byte[] one = fs.peek(victim, at, 1);
                one[0] ^= (byte) (1 << random.nextInt(8));
                fs.overwrite(victim, at, one);
            } else if (kind == 1) {
                byte[] garbage = new byte[Math.min(1 + random.nextInt(64), size - at)];
                random.nextBytes(garbage);
                if (Arrays.equals(garbage, fs.peek(victim, at, garbage.length))) {
                    garbage[0] ^= 1;
                }
                fs.overwrite(victim, at, garbage);
            } else {
                // The bytes of another segment at the same place: a misdirected write.
                Path other = files.get((files.indexOf(victim) + 1) % (files.size() - 1));
                int length = (int) Math.min(Math.min(64, size - at), fs.size(other) - Math.min(at, fs.size(other)));
                if (other.equals(victim) || length <= 0) {
                    fs.overwrite(victim, at, new byte[] {(byte) (fs.peek(victim, at, 1)[0] ^ 0x55)});
                } else {
                    fs.overwrite(victim, at, fs.peek(other, at, length));
                }
            }
            try (RaftLogStore store = open(fs)) {
                assertFalse(store.firstDamagedIndex() == -1, "damage at " + at + " of " + victim.getFileName()
                        + " (kind " + kind + ") went unnoticed");
                // What the store still holds is what was written.
                assertEquals(shapes(batch(1, (int) store.lastIndex(), 1, 250)), content(store, 1));
            }
        });
    }

    @Test
    void theLogStartsAnewAfterASnapshot() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        ClusterConfig other = ClusterConfig.ofVoters(3, 4, 5);
        try (RaftLogStore store = open(fs)) {
            for (int i = 0; i < 8; i++) {
                store.append(batch(1 + 5 * i, 5, 1, 300));
                store.sync();
            }
            assertTrue(store.segmentCount() > 3);
            assertTrue(store.storedBytes() > 40 * 300);
            long highest = Long.parseLong(segmentFiles(fs).getLast().getFileName().toString().substring(0, 10));

            store.reset(500, other);
            assertEquals(501, store.firstIndex());
            assertEquals(500, store.lastIndex());
            assertEquals(1, store.segmentCount());
            assertEquals(other, store.configAtEnd());
            assertTrue(store.storedBytes() < 300);
            assertEquals(List.of(String.format("%010d.rlog", highest + 1)),
                    segmentFiles(fs).stream().map(p -> p.getFileName().toString()).toList(),
                    "segment numbers are not used again");
            store.append(batch(501, 10, 7, 300));
            store.sync();
            assertEquals(shapes(batch(501, 10, 7, 300)), content(store, 501));
        }
        try (RaftLogStore store = RaftLogStore.open(fs, DIR, SEGMENT, 500, other)) {
            assertEquals(-1, store.firstDamagedIndex());
            assertEquals(501, store.firstIndex());
            assertEquals(510, store.lastIndex());
            assertEquals(shapes(batch(501, 10, 7, 300)), content(store, 501));
        }
        // What a node does at its start if it crashed during an installation.
        RaftLogStore.deleteSegments(fs, DIR);
        assertEquals(List.of(), segmentFiles(fs));
        RaftLogStore.deleteSegments(fs, DIR);
        try (RaftLogStore store = RaftLogStore.open(fs, DIR, SEGMENT, 900, THREE)) {
            assertEquals(901, store.firstIndex());
            assertEquals(900, store.lastIndex());
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void aMissingSegmentIsDamage() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        try (RaftLogStore store = open(fs)) {
            for (int i = 0; i < 10; i++) {
                store.append(batch(store.lastIndex() + 1, 2, 1, 250));
                store.sync();
            }
        }
        List<Path> files = segmentFiles(fs);
        assertTrue(files.size() >= 3, files.toString());
        // A segment in the middle: its successor does not follow what is left.
        fs.delete(files.get(1));
        fs.syncDirectory(DIR);
        try (RaftLogStore store = open(fs)) {
            assertEquals(store.lastIndex() + 1, store.firstDamagedIndex());
            assertTrue(store.lastIndex() > 0 && store.lastIndex() < 20);
            assertEquals(files.size() - 2, store.damagedFiles().size());
        }
        // The first segments are gone, and no snapshot covers what they held.
        fs.delete(files.get(0));
        fs.syncDirectory(DIR);
        try (RaftLogStore store = open(fs)) {
            assertEquals(1, store.firstDamagedIndex());
            assertEquals(0, store.lastIndex());
        }
    }

    @Test
    void aSegmentOfAnotherFormatVersionIsRejected() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        try (RaftLogStore store = open(fs)) {
            store.append(batch(1, 1, 1, 10));
            store.sync();
        }
        byte[] header = RaftLogStore.encodeHeader(new RaftLogStore.SegmentHeader(1, 0, 0, THREE));
        header[1] = 2;
        assertThrows(UnsupportedFormatException.class, () -> RaftLogStore.decodeHeader(header, "x"));
    }
}
