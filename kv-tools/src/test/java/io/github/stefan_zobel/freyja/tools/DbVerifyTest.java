package io.github.stefan_zobel.freyja.tools;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.github.stefan_zobel.freyja.lsm.EngineOptions;
import io.github.stefan_zobel.freyja.lsm.LsmEngine;
import io.github.stefan_zobel.freyja.lsm.WriteBatch;
import io.github.stefan_zobel.freyja.lsm.fs.RealFileSystem;

/** Runs the directory check tool against engine directories on the real file system. */
class DbVerifyTest {

    @TempDir
    Path temp;

    private final RealFileSystem fs = new RealFileSystem();
    private final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();

    private int run(String... args) {
        outBytes.reset();
        errBytes.reset();
        return DbVerify.run(args, fs,
                new PrintStream(outBytes, true, StandardCharsets.UTF_8),
                new PrintStream(errBytes, true, StandardCharsets.UTF_8));
    }

    private String out() {
        return outBytes.toString(StandardCharsets.UTF_8);
    }

    private static EngineOptions options() {
        return new EngineOptions().deterministic(true).reserveSize(0).minFreeSpace(0)
                .blockCacheCapacity(0).level0CompactionTrigger(2);
    }

    /** An engine directory with tables in two levels and three batches in its log. */
    private Path build() throws IOException {
        Path dir = temp.resolve("db");
        try (LsmEngine engine = LsmEngine.open(fs, dir, options())) {
            for (int table = 0; table < 3; table++) {
                for (int i = 0; i < 50; i++) {
                    engine.write(new WriteBatch().put(("t" + table + "-" + i).getBytes(), new byte[100]), false);
                }
                engine.flush();
                if (table == 1) {
                    engine.compact();
                }
            }
            engine.put("a".getBytes(), "1".getBytes());
            engine.put("b".getBytes(), "2".getBytes());
            engine.put("c".getBytes(), "3".getBytes());
        }
        return dir;
    }

    private static List<Path> files(Path dir, String suffix) throws IOException {
        try (Stream<Path> stream = Files.list(dir)) {
            return stream.filter(p -> p.getFileName().toString().endsWith(suffix)).sorted().toList();
        }
    }

    private static byte[] snapshotOf(Path dir) throws IOException {
        ByteArrayOutputStream all = new ByteArrayOutputStream();
        try (Stream<Path> stream = Files.list(dir)) {
            for (Path path : stream.sorted().toList()) {
                all.writeBytes(path.getFileName().toString().getBytes(StandardCharsets.UTF_8));
                all.writeBytes(Files.readAllBytes(path));
            }
        }
        return all.toByteArray();
    }

    @Test
    void anIntactDirectoryIsReportedAsIntactAndIsNotChanged() throws IOException {
        Path dir = build();
        byte[] before = snapshotOf(dir);
        assertEquals(0, run(dir.toString()), out());
        String text = out();
        assertTrue(text.contains("tables:       2 with 150 entries"), text);
        assertTrue(text.contains("logs:         1 with 3 batches"), text);
        assertTrue(text.contains("result:       intact"), text);
        assertArrayEquals(before, snapshotOf(dir), "the tool changes nothing");
        // The engine can still open the directory: the tool took no lock and left none.
        try (LsmEngine engine = LsmEngine.open(fs, dir, options())) {
            assertEquals(153, engine.lastSequence());
        }
    }

    @Test
    void damageInATableIsFoundWithFileAndOffset() throws IOException {
        Path dir = build();
        Path table = files(dir, ".sst").get(0);
        byte[] bytes = Files.readAllBytes(table);
        bytes[4500] ^= 0x20;
        Files.write(table, bytes);

        assertEquals(1, run(dir.toString()));
        String text = out();
        assertTrue(text.contains("DAMAGE:"), text);
        assertTrue(text.contains(table.getFileName().toString()), text);
        assertTrue(text.contains("checksum mismatch"), text);
        assertTrue(text.contains("result:       1 problems found"), text);
    }

    @Test
    void damageInTheLogAndAMissingTableAreFound() throws IOException {
        Path dir = build();
        Path log = files(dir, ".wal").get(0);
        byte[] bytes = Files.readAllBytes(log);
        bytes[25] ^= 0x01; // the first batch; later sync points prove it was synced
        Files.write(log, bytes);
        Files.delete(files(dir, ".sst").get(1));

        assertEquals(1, run(dir.toString()));
        String text = out();
        assertTrue(text.contains("a table listed in the manifest is missing"), text);
        assertTrue(text.contains(log.getFileName().toString()), text);
        assertTrue(text.contains("result:       2 problems found"), text);
    }

    @Test
    void aTornLogTailAndLeftoverFilesAreNotesNotDamage() throws IOException {
        Path dir = build();
        Path log = files(dir, ".wal").get(0);
        byte[] bytes = Files.readAllBytes(log);
        Files.write(log, java.util.Arrays.copyOf(bytes, bytes.length - 3));
        Files.write(dir.resolve("000999.sst"), new byte[] {1, 2, 3});

        assertEquals(0, run(dir.toString()), out());
        String text = out();
        assertTrue(text.contains("ends in a torn tail"), text);
        assertTrue(text.contains("000999.sst is not listed in the manifest"), text);
        assertTrue(text.contains("logs:         1 with 2 batches"), text);
        assertTrue(text.contains("result:       intact"), text);
    }

    @Test
    void aMissingOrDamagedCurrentFileIsFound() throws IOException {
        Path dir = build();
        Path current = dir.resolve("CURRENT");
        byte[] bytes = Files.readAllBytes(current);
        bytes[20] ^= 0x01;
        Files.write(current, bytes);
        assertEquals(1, run(dir.toString()));
        assertTrue(out().contains("CURRENT"), out());

        Files.delete(current);
        assertEquals(1, run(dir.toString()));
        assertTrue(out().contains("CURRENT is missing"), out());
    }

    @Test
    void anEmptyDirectoryAndUsageErrors() throws IOException {
        Path empty = temp.resolve("empty");
        Files.createDirectories(empty);
        assertEquals(0, run(empty.toString()), out());
        assertTrue(out().contains("an empty or new directory"), out());

        assertEquals(1, run(temp.resolve("does-not-exist").toString()));
        assertEquals(2, run());
        assertEquals(2, run("--levels", "1", empty.toString()));
        assertEquals(2, run("--bogus", empty.toString()));
        assertEquals(2, run(empty.toString(), empty.toString()));
    }
}
