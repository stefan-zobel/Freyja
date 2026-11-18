package io.github.sz.freyja.node.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.common.testing.Seeds;
import io.github.sz.freyja.lsm.UnsupportedFormatException;
import io.github.sz.freyja.lsm.fs.FaultyFileSystem;
import io.github.sz.freyja.lsm.fs.FaultyFileSystem.SimulatedCrash;
import io.github.sz.freyja.lsm.fs.MemoryFileSystem;
import io.github.sz.freyja.lsm.fs.ReadableFile;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.lsm.fs.WritableFile;
import io.github.sz.freyja.raft.HardState;

/** The identity file and the two hard-state files (docs/spec/node.md, sections 2, 3.2, 3.4). */
class HardStateStoreTest {

    private static final Path DIR = Path.of("data");
    private static final Path A = DIR.resolve(HardStateStore.FILE_A);
    private static final Path B = DIR.resolve(HardStateStore.FILE_B);

    private static MemoryFileSystem newDirectory() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        fs.createDirectories(DIR);
        return fs;
    }

    private static byte[] read(StorageFileSystem fs, Path path) throws IOException {
        try (ReadableFile file = fs.open(path)) {
            byte[] bytes = new byte[(int) file.size()];
            file.readFully(0, bytes, 0, bytes.length);
            return bytes;
        }
    }

    /** Replaces a file by other bytes, durably, as damage that happened behind the back of the node. */
    private static void overwrite(StorageFileSystem fs, Path path, byte[] bytes) throws IOException {
        fs.delete(path);
        try (WritableFile file = fs.create(path)) {
            file.append(ByteBuffer.wrap(bytes));
            file.sync();
        }
        fs.syncDirectory(path.getParent());
    }

    private static HardState state(int n) {
        return new HardState(10 + n, n % 3, n % 2 == 0, n, 100L * n, n == 7 ? HardState.UNKNOWN_TERM : n);
    }

    @Test
    void theStateSurvivesAReopenWithAllItsFields() throws IOException {
        MemoryFileSystem fs = newDirectory();
        HardStateStore store = HardStateStore.initialize(fs, DIR);
        assertEquals(HardState.INITIAL, store.get());
        assertEquals(1, store.counter());
        assertEquals(HardState.INITIAL, HardStateStore.open(fs, DIR).get());

        for (int n = 1; n <= 8; n++) {
            store.put(state(n));
            assertEquals(state(n), store.get());
            HardStateStore reopened = HardStateStore.open(fs, DIR);
            assertEquals(state(n), reopened.get());
            assertEquals(n + 1, reopened.counter());
        }
        assertEquals(List.of(HardStateStore.FILE_A, HardStateStore.FILE_B),
                fs.list(DIR).stream().map(p -> p.getFileName().toString()).sorted().toList(),
                "no temporary file is left");
        assertEquals(read(fs, A).length, read(fs, B).length);
        assertFalse(java.util.Arrays.equals(read(fs, A), read(fs, B)), "each record is bound to its file");
        assertThrows(IOException.class, () -> HardStateStore.initialize(fs, DIR), "only once");
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void aCrashAtAnyPointOfAnUpdateLeavesTheOldOrTheNewState() {
        Seeds.forEach(HardStateStoreTest.class, "hard-state-crash", 20, seed -> {
            HardState old = state(3);
            HardState next = state(4);
            // Count the operations of one update.
            FaultyFileSystem counting = new FaultyFileSystem(newDirectory());
            HardStateStore.initialize(counting, DIR).put(old);
            long before = counting.operationCount();
            HardStateStore.open(counting, DIR).put(next);
            long operations = counting.operationCount() - before;
            assertTrue(operations >= 8, "operations of an update: " + operations);

            for (long at = 0; at < operations; at++) {
                MemoryFileSystem memory = newDirectory();
                FaultyFileSystem faulty = new FaultyFileSystem(memory);
                HardStateStore store = HardStateStore.initialize(faulty, DIR);
                store.put(old);
                Random random = new Random(seed * 1_000_003 + at);
                faulty.crashAt(faulty.operationCount() + at, random, () -> memory.crash(random));
                try {
                    store.put(next);
                    fail("no crash at operation " + at + " of " + operations);
                } catch (SimulatedCrash expected) {
                    // the node is gone
                }
                HardStateStore recovered = HardStateStore.open(memory, DIR);
                assertTrue(recovered.get().equals(old) || recovered.get().equals(next),
                        "a crash at operation " + at + " left " + recovered.get());
                // The recovery made both files hold the state again, and a new update works.
                assertEquals(recovered.get(), HardStateStore.open(memory, DIR).get());
                recovered.put(state(5));
                assertEquals(state(5), HardStateStore.open(memory, DIR).get());
                assertEquals(0, memory.openHandleCount());
            }
        });
    }

    @Test
    void anUpdateThatReturnedIsNotLostInACrash() throws IOException {
        for (int seed = 0; seed < 50; seed++) {
            MemoryFileSystem fs = newDirectory();
            HardStateStore store = HardStateStore.initialize(fs, DIR);
            store.put(state(1));
            store.put(state(2));
            fs.crash(new Random(seed));
            assertEquals(state(2), HardStateStore.open(fs, DIR).get());
        }
    }

    @Test
    void oneDamagedFileIsRepairedFromTheOtherAndTwoAreALostIdentity() throws IOException {
        MemoryFileSystem fs = newDirectory();
        HardStateStore.initialize(fs, DIR).put(state(6));
        byte[] a = read(fs, A);
        byte[] b = read(fs, B);

        // Every single bit flip in A: B counts, and A is written again.
        for (int bit = 0; bit < a.length * 8; bit++) {
            byte[] damaged = a.clone();
            damaged[bit / 8] ^= (byte) (1 << (bit % 8));
            overwrite(fs, A, damaged);
            overwrite(fs, B, b);
            if (bit / 8 == 4) {
                // The version byte: the checksum cannot hold, so the file is invalid, not unknown.
                assertEquals(state(6), HardStateStore.open(fs, DIR).get());
                continue;
            }
            HardStateStore store = HardStateStore.open(fs, DIR);
            assertEquals(state(6), store.get(), "bit " + bit);
            assertEquals(state(6), HardStateStore.open(fs, DIR).get());
        }
        // The same for B.
        overwrite(fs, A, a);
        byte[] truncated = java.util.Arrays.copyOf(b, b.length - 1);
        overwrite(fs, B, truncated);
        assertEquals(state(6), HardStateStore.open(fs, DIR).get());
        fs.delete(B);
        assertEquals(state(6), HardStateStore.open(fs, DIR).get());
        assertTrue(fs.exists(B), "the missing file is written again");

        // The record of one file is not valid in the other.
        byte[] freshA = read(fs, A);
        byte[] freshB = read(fs, B);
        overwrite(fs, A, freshB);
        overwrite(fs, B, freshA);
        assertThrows(LostIdentityException.class, () -> HardStateStore.open(fs, DIR));

        overwrite(fs, A, new byte[freshA.length]);
        overwrite(fs, B, freshB);
        fs.delete(B);
        assertThrows(LostIdentityException.class, () -> HardStateStore.open(fs, DIR));
        fs.delete(A);
        assertThrows(LostIdentityException.class, () -> HardStateStore.open(fs, DIR), "an empty directory");
    }

    @Test
    void theFileWithTheHigherCounterCounts() throws IOException {
        MemoryFileSystem fs = newDirectory();
        HardStateStore store = HardStateStore.initialize(fs, DIR);
        store.put(state(1));
        byte[] oldB = read(fs, B);
        store.put(state(2));
        // As after a crash between the two files: A is new, B is old.
        overwrite(fs, B, oldB);
        HardStateStore reopened = HardStateStore.open(fs, DIR);
        assertEquals(state(2), reopened.get());
        // And the other way round, which a crash cannot produce but damage and repair can.
        byte[] newA = read(fs, A);
        store = HardStateStore.open(fs, DIR);
        store.put(state(3));
        overwrite(fs, A, newA);
        assertEquals(state(3), HardStateStore.open(fs, DIR).get());
    }

    @Test
    void aRecordOfAnotherFormatVersionIsRejectedNotGuessed() throws IOException {
        MemoryFileSystem fs = newDirectory();
        HardStateStore.initialize(fs, DIR);
        byte[] body = new byte[45];
        overwrite(fs, A, SmallFile.encode(HardStateStore.MAGIC, 2, body, 1));
        assertThrows(UnsupportedFormatException.class, () -> HardStateStore.open(fs, DIR));
    }

    @Test
    void aFailedUpdateIsFatalForTheInstance() throws IOException {
        MemoryFileSystem memory = newDirectory();
        FaultyFileSystem faulty = new FaultyFileSystem(memory);
        HardStateStore store = HardStateStore.initialize(faulty, DIR);
        faulty.setFault((operation, path, index) ->
                operation == FaultyFileSystem.Operation.SYNC ? new IOException("injected") : null);
        assertThrows(IOException.class, () -> store.put(state(1)));
        faulty.setFault(null);
        IOException again = assertThrows(IOException.class, () -> store.put(state(1)));
        assertTrue(again.getMessage().contains("must stop"), again.getMessage());
        assertEquals(HardState.INITIAL, store.get());
    }

    @Test
    void theIdentityIsWrittenOnceAndItsLossIsNoticed() throws IOException {
        MemoryFileSystem fs = newDirectory();
        assertFalse(NodeIdentity.exists(fs, DIR));
        assertThrows(LostIdentityException.class, () -> NodeIdentity.read(fs, DIR));
        NodeIdentity identity = new NodeIdentity(0xC0FFEE_0123456789L, 7);
        identity.write(fs, DIR);
        assertTrue(NodeIdentity.exists(fs, DIR));
        fs.crash();
        assertEquals(identity, NodeIdentity.read(fs, DIR));
        assertThrows(IOException.class, () -> new NodeIdentity(1, 2).write(fs, DIR), "never a second time");

        Path file = DIR.resolve(NodeIdentity.FILE_NAME);
        byte[] bytes = read(fs, file);
        for (int bit = 0; bit < bytes.length * 8; bit++) {
            if (bit / 8 == 4) {
                continue;
            }
            byte[] damaged = bytes.clone();
            damaged[bit / 8] ^= (byte) (1 << (bit % 8));
            overwrite(fs, file, damaged);
            assertThrows(LostIdentityException.class, () -> NodeIdentity.read(fs, DIR));
        }
        overwrite(fs, file, bytes);
        assertEquals(identity, NodeIdentity.read(fs, DIR));
        assertEquals(0, fs.openHandleCount());
    }
}
