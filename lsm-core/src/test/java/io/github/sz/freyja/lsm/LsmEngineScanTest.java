package io.github.sz.freyja.lsm;

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

import io.github.sz.freyja.lsm.fs.MemoryFileSystem;
import io.github.sz.freyja.lsm.fs.ReadableFile;
import io.github.sz.freyja.lsm.fs.WritableFile;

/** Snapshots and range scans. */
class LsmEngineScanTest {

    private final MemoryFileSystem fs = new MemoryFileSystem();
    private final Path dir = Path.of("scan-test").toAbsolutePath();
    private final EngineOptions options = new EngineOptions().reserveSize(0).minFreeSpace(0).deterministic(true).blockCacheCapacity(1 << 20);

    private static byte[] b(String text) {
        return text == null ? null : text.getBytes(StandardCharsets.UTF_8);
    }

    private static String s(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    private LsmEngine open() throws IOException {
        return LsmEngine.open(fs, dir, options);
    }

    /** Collects a scan as "key=value" strings and closes it. */
    private static List<String> collect(ScanIterator scan) throws IOException {
        List<String> result = new ArrayList<>();
        try (scan) {
            for (; scan.valid(); scan.next()) {
                result.add(s(scan.key()) + "=" + s(scan.value()));
            }
        }
        return result;
    }

    // ---- scans -----------------------------------------------------------------------------

    @Test
    void aScanReturnsTheNewestValuesInKeyOrderAcrossAllSources() throws IOException {
        try (LsmEngine engine = open()) {
            // Oldest table.
            engine.put(b("a"), b("a1"));
            engine.put(b("c"), b("c1"));
            engine.put(b("e"), b("e1"));
            engine.flush();
            // Newer table: overwrites, deletes, adds.
            engine.put(b("c"), b("c2"));
            engine.delete(b("e"));
            engine.put(b("b"), b("b2"));
            engine.flush();
            // MemTable.
            engine.put(b("a"), b("a3"));
            engine.put(b("d"), b("d3"));
            engine.delete(b("b"));
            engine.put(b("f"), b("f3"));

            assertEquals(List.of("a=a3", "c=c2", "d=d3", "f=f3"), collect(engine.scan(null, null)));
            assertEquals(List.of("f=f3", "d=d3", "c=c2", "a=a3"), collect(engine.scanReverse(null, null)));
            assertEquals(0, engine.openScanCount());
            assertEquals(0, engine.blockCache().pinnedCount());
        }
    }

    @Test
    void aScanHonorsItsBounds() throws IOException {
        try (LsmEngine engine = open()) {
            for (String key : new String[] {"a", "b", "bb", "c", "d"}) {
                engine.put(b(key), b(key.toUpperCase()));
            }
            engine.flush();
            engine.put(b("ca"), b("CA"));

            assertEquals(List.of("b=B", "bb=BB", "c=C", "ca=CA"), collect(engine.scan(b("b"), b("d"))));
            assertEquals(List.of("bb=BB", "c=C"), collect(engine.scan(b("ba"), b("ca"))), "start between keys, end exclusive");
            assertEquals(List.of("a=A", "b=B"), collect(engine.scan(null, b("bb"))));
            assertEquals(List.of("ca=CA", "d=D"), collect(engine.scan(b("ca"), null)));
            assertEquals(List.of(), collect(engine.scan(b("x"), null)));
            assertEquals(List.of(), collect(engine.scan(b("c"), b("c"))), "an empty range");
            assertEquals(List.of(), collect(engine.scan(b("d"), b("b"))), "an inverted range");
            // Backward the range is the same: its end stays exclusive, its start inclusive.
            assertEquals(List.of("ca=CA", "c=C", "bb=BB", "b=B"), collect(engine.scanReverse(b("b"), b("d"))));
            assertEquals(List.of("c=C", "bb=BB"), collect(engine.scanReverse(b("ba"), b("ca"))));
            assertEquals(List.of("b=B", "a=A"), collect(engine.scanReverse(null, b("bb"))));
            assertEquals(List.of("d=D", "ca=CA"), collect(engine.scanReverse(b("ca"), null)));
            assertEquals(List.of(), collect(engine.scanReverse(b("x"), null)));
            assertEquals(List.of(), collect(engine.scanReverse(b("c"), b("c"))));
            assertEquals(List.of(), collect(engine.scanReverse(b("d"), b("b"))));
            assertThrows(IllegalArgumentException.class, () -> engine.scanReverse(new byte[0], null));
            assertThrows(IllegalArgumentException.class, () -> engine.scan(new byte[0], null));
            assertThrows(IllegalArgumentException.class, () -> engine.scan(null, new byte[0]));
        }
    }

    @Test
    void aScanOfAnEmptyEngineIsEmpty() throws IOException {
        try (LsmEngine engine = open()) {
            ScanIterator scan = engine.scan(null, null);
            assertFalse(scan.valid());
            assertThrows(IllegalStateException.class, scan::key);
            assertThrows(IllegalStateException.class, scan::next);
            scan.close();
            scan.close();
            assertEquals(0, engine.openScanCount());
        }
    }

    @Test
    void manyVersionsOfOneKeyYieldOneEntry() throws IOException {
        try (LsmEngine engine = open()) {
            for (int i = 0; i < 300; i++) {
                engine.write(new WriteBatch().put(b("hot"), b("v" + i)).put(b("k" + (i % 7)), b("x" + i)), false);
                if (i % 50 == 49) {
                    engine.flush();
                }
            }
            List<String> all = collect(engine.scan(null, null));
            assertEquals(8, all.size());
            assertEquals("hot=v299", all.get(0));
            assertEquals("k6=x293", all.get(7));
        }
    }

    @Test
    void anOpenScanIsNotDisturbedByLaterWritesAndFlushes() throws IOException {
        try (LsmEngine engine = open()) {
            for (int i = 0; i < 100; i++) {
                engine.write(new WriteBatch().put(b(String.format("key-%03d", i)), b("old")), false);
            }
            ScanIterator scan = engine.scan(null, null);
            int seen = 0;
            for (; scan.valid() && seen < 30; scan.next()) {
                assertEquals("old", s(scan.value()));
                seen++;
            }
            // Overwrite, delete, and insert while the scan is open; flush twice.
            for (int i = 0; i < 100; i++) {
                engine.write(new WriteBatch().put(b(String.format("key-%03d", i)), b("new")), false);
            }
            engine.flush();
            engine.delete(b("key-050"));
            engine.put(b("key-0505"), b("inserted"));
            engine.flush();

            for (; scan.valid(); scan.next()) {
                assertEquals(String.format("key-%03d", seen), s(scan.key()));
                assertEquals("old", s(scan.value()));
                seen++;
            }
            assertEquals(100, seen);
            scan.close();

            List<String> now = collect(engine.scan(b("key-049"), b("key-052")));
            assertEquals(List.of("key-049=new", "key-0505=inserted", "key-051=new"), now);
        }
    }

    @Test
    void aDamagedBlockFailsTheScanThatReachesIt() throws IOException {
        try (LsmEngine engine = open()) {
            for (int i = 0; i < 200; i++) {
                engine.write(new WriteBatch().put(b(String.format("key-%03d", i)), new byte[100]), false);
            }
            engine.flush();
        }
        Path table = null;
        for (Path path : fs.list(dir)) {
            if (path.getFileName().toString().endsWith(".sst")) {
                table = path;
            }
        }
        byte[] bytes;
        try (ReadableFile file = fs.open(table)) {
            bytes = new byte[(int) file.size()];
            file.readFully(0, bytes, 0, bytes.length);
        }
        bytes[9000] ^= 0x01; // a data block in the middle
        fs.delete(table);
        try (WritableFile file = fs.create(table)) {
            file.append(bytes, 0, bytes.length);
            file.sync();
        }

        try (LsmEngine engine = open()) {
            ScanIterator scan = engine.scan(null, null);
            CorruptionException e = assertThrows(CorruptionException.class, () -> {
                while (scan.valid()) {
                    scan.next();
                }
            });
            assertTrue(e.offset() > 0 && e.offset() <= 9000, "offset " + e.offset());
            scan.close();
            assertEquals(0, engine.blockCache().pinnedCount());
            // Keys before the damaged block are still readable.
            assertEquals(100, engine.get(b("key-000")).length);
        }
    }

    // ---- snapshots -------------------------------------------------------------------------

    @Test
    void aSnapshotKeepsSeeingTheStateAtItsCreation() throws IOException {
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("1"));
            engine.put(b("b"), b("1"));
            Snapshot first = engine.snapshot();
            assertEquals(2, first.sequence());

            engine.put(b("a"), b("2"));
            engine.delete(b("b"));
            engine.put(b("c"), b("2"));
            Snapshot second = engine.snapshot();
            engine.flush();
            engine.put(b("a"), b("3"));
            engine.put(b("b"), b("3"));
            engine.flush();
            engine.delete(b("c"));

            assertEquals("1", s(engine.get(b("a"), first)));
            assertEquals("1", s(engine.get(b("b"), first)));
            assertNull(engine.get(b("c"), first));
            assertEquals(List.of("a=1", "b=1"), collect(engine.scan(null, null, first)));

            assertEquals("2", s(engine.get(b("a"), second)));
            assertNull(engine.get(b("b"), second));
            assertEquals("2", s(engine.get(b("c"), second)));
            assertEquals(List.of("a=2", "c=2"), collect(engine.scan(null, null, second)));
            assertEquals(List.of("c=2", "a=2"), collect(engine.scanReverse(null, null, second)));
            assertEquals(List.of("b=1", "a=1"), collect(engine.scanReverse(null, null, first)));

            assertEquals(List.of("a=3", "b=3"), collect(engine.scan(null, null)));
            first.close();
            second.close();
        }
    }

    @Test
    void aSnapshotNeverSeesPartOfABatch() throws IOException {
        try (LsmEngine engine = open()) {
            engine.write(new WriteBatch().put(b("a"), b("1")).put(b("b"), b("1")), true);
            try (Snapshot snapshot = engine.snapshot()) {
                engine.write(new WriteBatch().put(b("a"), b("2")).put(b("b"), b("2")).put(b("c"), b("2")), true);
                assertEquals(List.of("a=1", "b=1"), collect(engine.scan(null, null, snapshot)));
            }
        }
    }

    @Test
    void liveSnapshotsAreTracked() throws IOException {
        try (LsmEngine engine = open()) {
            engine.put(b("a"), b("1"));
            assertEquals(0, engine.liveSnapshotCount());
            assertEquals(1, engine.oldestSnapshotSequence(), "without snapshots: the last sequence");

            Snapshot one = engine.snapshot();
            Snapshot alsoOne = engine.snapshot();
            engine.put(b("a"), b("2"));
            Snapshot two = engine.snapshot();
            engine.put(b("a"), b("3"));
            assertEquals(3, engine.liveSnapshotCount());
            assertEquals(1, engine.oldestSnapshotSequence());

            one.close();
            one.close(); // closing twice counts once
            assertEquals(2, engine.liveSnapshotCount());
            assertEquals(1, engine.oldestSnapshotSequence(), "another snapshot at 1 is still live");
            alsoOne.close();
            assertEquals(2, engine.oldestSnapshotSequence());
            two.close();
            assertEquals(0, engine.liveSnapshotCount());
            assertEquals(3, engine.oldestSnapshotSequence());

            assertThrows(IllegalStateException.class, () -> engine.get(b("a"), one));
            assertThrows(IllegalStateException.class, () -> engine.scan(null, null, two));
        }
    }

    @Test
    void aSnapshotOfAnotherEngineIsRejected() throws IOException {
        try (LsmEngine engine = open();
             LsmEngine other = LsmEngine.open(fs, Path.of("scan-test-other").toAbsolutePath(), options)) {
            try (Snapshot foreign = other.snapshot()) {
                assertThrows(IllegalArgumentException.class, () -> engine.get(b("a"), foreign));
                assertThrows(IllegalArgumentException.class, () -> engine.scan(null, null, foreign));
            }
        }
    }
}
