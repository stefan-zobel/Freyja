package io.github.stefan_zobel.freyja.lsm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.github.stefan_zobel.freyja.lsm.fs.MemoryFileSystem;
import io.github.stefan_zobel.freyja.lsm.fs.RealFileSystem;

/** Checkpoints: consistent copies of an engine directory made of hard links. */
class LsmEngineCheckpointTest {

    private final MemoryFileSystem fs = new MemoryFileSystem();
    private final Path dir = Path.of("checkpoint-test", "db").toAbsolutePath();
    private final Path copy = Path.of("checkpoint-test", "copy").toAbsolutePath();
    private final EngineOptions options = new EngineOptions().reserveSize(0).minFreeSpace(0).deterministic(true)
            .blockCacheCapacity(1 << 20)
            .level0CompactionTrigger(2);

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String s(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
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

    private List<String> names(Path directory) throws IOException {
        List<String> result = new ArrayList<>();
        for (Path path : fs.list(directory)) {
            result.add(path.getFileName().toString());
        }
        return result;
    }

    @Test
    void aCheckpointHoldsEverythingWrittenBeforeIt() throws IOException {
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            engine.put(b("a"), b("1"));
            engine.flush();
            engine.put(b("b"), b("2"));
            engine.write(new WriteBatch().put(b("c"), b("3")), false); // only in the MemTable

            engine.checkpoint(copy);

            // The copy consists of table files, a manifest, and CURRENT; no log, no lock.
            List<String> files = names(copy);
            assertTrue(files.contains("CURRENT"), files.toString());
            assertEquals(2, files.stream().filter(n -> n.endsWith(".sst")).count(), files.toString());
            assertEquals(1, files.stream().filter(n -> n.startsWith("MANIFEST-")).count(), files.toString());
            assertEquals(4, files.size(), files.toString());

            // The source goes on and diverges.
            engine.put(b("a"), b("changed"));
            engine.delete(b("b"));
            engine.put(b("d"), b("4"));
            engine.flush();
            assertTrue(engine.compact());

            try (LsmEngine other = LsmEngine.open(fs, copy, options)) {
                assertEquals(List.of("a=1", "b=2", "c=3"), collect(other.scan(null, null)));
                assertEquals(3, other.lastSequence());
                // The copy is a full engine: it accepts writes without touching the source.
                other.put(b("only-in-copy"), b("x"));
                other.flush();
                other.put(b("a"), b("copy"));
                other.flush();
                assertTrue(other.compact());
                assertEquals("copy", s(other.get(b("a"))));
            }
            assertEquals(List.of("a=changed", "c=3", "d=4"), collect(engine.scan(null, null)));
            assertNull(engine.get(b("only-in-copy")));
        }
        try (LsmEngine other = LsmEngine.open(fs, copy, options)) {
            assertEquals(List.of("a=copy", "b=2", "c=3", "only-in-copy=x"), collect(other.scan(null, null)));
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void aCheckpointSurvivesTheDeletionOfItsSourceFiles() throws IOException {
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            for (int i = 0; i < 100; i++) {
                engine.write(new WriteBatch().put(b("key-" + i), b("v" + i)), false);
            }
            engine.checkpoint(copy);
            // Rewrite everything in the source; compaction deletes the files the copy links to.
            for (int i = 0; i < 100; i++) {
                engine.write(new WriteBatch().delete(b("key-" + i)), false);
            }
            engine.flush();
            assertTrue(engine.compact());
            assertEquals(List.of(), collect(engine.scan(null, null)));
            assertEquals(0, names(dir).stream().filter(n -> n.endsWith(".sst")).count());
        }
        try (LsmEngine other = LsmEngine.open(fs, copy, options)) {
            assertEquals(100, collect(other.scan(null, null)).size());
            assertEquals("v42", s(other.get(b("key-42"))));
        }
    }

    @Test
    void aCheckpointIsDurableAndTheSourceSurvivesACrash() throws IOException {
        LsmEngine engine = LsmEngine.open(fs, dir, options);
        engine.put(b("a"), b("1"));
        engine.write(new WriteBatch().put(b("b"), b("2")), false);
        engine.checkpoint(copy);
        engine.write(new WriteBatch().put(b("c"), b("3")), false);
        fs.crash();

        try (LsmEngine other = LsmEngine.open(fs, copy, options)) {
            assertEquals(List.of("a=1", "b=2"), collect(other.scan(null, null)));
        }
        try (LsmEngine reopened = LsmEngine.open(fs, dir, options)) {
            // The checkpoint flushed the MemTable, so "b" became durable; "c" was lost.
            assertEquals(List.of("a=1", "b=2"), collect(reopened.scan(null, null)));
        }
    }

    @Test
    void anEmptyEngineCanBeCheckpointed() throws IOException {
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            engine.checkpoint(copy);
        }
        try (LsmEngine other = LsmEngine.open(fs, copy, options)) {
            assertEquals(List.of(), collect(other.scan(null, null)));
            other.put(b("a"), b("1"));
            assertEquals("1", s(other.get(b("a"))));
        }
    }

    @Test
    void theTargetMustNotExist() throws IOException {
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            engine.put(b("a"), b("1"));
            fs.createDirectories(copy);
            IOException e = assertThrows(IOException.class, () -> engine.checkpoint(copy));
            assertTrue(e.getMessage().contains("exists"), e.getMessage());
            assertFalse(e instanceof EngineFailedException);
            engine.put(b("b"), b("2")); // the engine is unaffected
        }
    }

    @Test
    void severalCheckpointsOfOneEngineAreIndependent() throws IOException {
        Path second = copy.resolveSibling("copy-2");
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            engine.put(b("a"), b("1"));
            engine.checkpoint(copy);
            engine.put(b("a"), b("2"));
            engine.checkpoint(second);
            engine.put(b("a"), b("3"));
            assertEquals("3", s(engine.get(b("a"))));
        }
        try (LsmEngine first = LsmEngine.open(fs, copy, options);
             LsmEngine other = LsmEngine.open(fs, second, options)) {
            assertEquals("1", s(first.get(b("a"))));
            assertEquals("2", s(other.get(b("a"))));
        }
    }

    @Test
    void aCheckpointWorksOnTheRealFileSystem(@TempDir Path temp) throws IOException {
        RealFileSystem real = new RealFileSystem();
        Path realDir = temp.resolve("db");
        Path realCopy = temp.resolve("copy");
        try (LsmEngine engine = LsmEngine.open(real, realDir, options)) {
            for (int i = 0; i < 50; i++) {
                engine.put(b("key-" + i), b("v" + i));
                if (i % 10 == 9) {
                    engine.flush();
                }
            }
            engine.checkpoint(realCopy);
            // Compaction deletes the linked files in the source while this engine and,
            // below, the copy are open.
            assertTrue(engine.compact());
            try (LsmEngine other = LsmEngine.open(real, realCopy, options)) {
                assertEquals(50, collect(other.scan(null, null)).size());
                assertEquals("v7", s(other.get(b("key-7"))));
            }
            assertEquals("v7", s(engine.get(b("key-7"))));
        }
        // Everything is closed: both directories can be deleted.
        try (var paths = Files.walk(temp)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                if (!path.equals(temp)) {
                    Files.delete(path);
                }
            }
        }
    }
}
