package io.github.sz.freyja.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.github.sz.freyja.lsm.EngineOptions;
import io.github.sz.freyja.lsm.LsmEngine;
import io.github.sz.freyja.lsm.WriteBatch;
import io.github.sz.freyja.lsm.fs.RealFileSystem;

/** Runs the log dump tool against logs written by the engine on the real file system. */
class WalDumpTest {

    @TempDir
    Path dir;

    private final RealFileSystem fs = new RealFileSystem();
    private final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();

    private int run(String... args) {
        outBytes.reset();
        errBytes.reset();
        return WalDump.run(args, fs,
                new PrintStream(outBytes, true, StandardCharsets.UTF_8),
                new PrintStream(errBytes, true, StandardCharsets.UTF_8));
    }

    private String out() {
        return outBytes.toString(StandardCharsets.UTF_8);
    }

    private String err() {
        return errBytes.toString(StandardCharsets.UTF_8);
    }

    /** Writes three batches through the engine and returns its first log. */
    private Path writeLog() throws IOException {
        Path engineDir = dir.resolve("db");
        try (LsmEngine engine = LsmEngine.open(fs, engineDir, new EngineOptions().reserveSize(0).minFreeSpace(0).deterministic(true).blockCacheCapacity(0))) {
            engine.put(new byte[] {0x61}, new byte[] {0x01, 0x02});
            engine.write(new WriteBatch()
                    .put(new byte[] {0x62}, new byte[100])
                    .delete(new byte[] {0x61}), true);
            engine.delete(new byte[] {0x63});
        }
        return engineDir.resolve("000001.wal");
    }

    @Test
    void printsRecordsSyncPointsAndACleanEnd() throws IOException {
        Path log = writeLog();
        assertEquals(0, run("--sealed", log.toString()), err());
        String text = out();
        assertTrue(text.contains("file number:  1"), text);
        assertTrue(text.contains("treated as:   sealed"), text);
        assertTrue(text.contains("kind:         WAL"), text);
        assertTrue(text.contains("record at 14, 24 bytes: batch, sequence 1, 1 entry"), text);
        assertTrue(text.contains("sync point at 45: 45 bytes synced"), text);
        assertTrue(text.contains(": batch, sequence 2 to 3, 2 entries"), text);
        assertTrue(text.contains(": batch, sequence 4, 1 entry"), text);
        assertTrue(text.contains("records:      3"), text);
        assertTrue(text.contains("sync points:  2"), text);
        assertTrue(text.contains("end:          clean"), text);
        assertFalse(text.contains("PUT"), "entries are printed only on request");
    }

    @Test
    void printsEveryEntryOnRequest() throws IOException {
        Path log = writeLog();
        assertEquals(0, run("--entries", log.toString()), err());
        String text = out();
        assertTrue(text.contains("  @1 PUT 61 -> 0102 (2 bytes)"), text);
        assertTrue(text.contains("  @2 PUT 62 -> " + "00".repeat(32) + "... (100 bytes)"), text);
        assertTrue(text.contains("  @3 DELETE 61"), text);
        assertTrue(text.contains("  @4 DELETE 63"), text);
        assertTrue(text.contains("treated as:   last log"), text);
    }

    @Test
    void aTornTailIsReportedButIsNotAnError() throws IOException {
        Path log = writeLog();
        byte[] bytes = Files.readAllBytes(log);
        Path torn = dir.resolve("000001.wal");
        Files.write(torn, Arrays.copyOf(bytes, bytes.length - 3));

        assertEquals(0, run(torn.toString()), err());
        String text = out();
        assertTrue(text.contains("records:      2"), text);
        assertTrue(text.contains("end:          torn tail at "), text);
        assertTrue(text.contains("valid length "), text);
        assertEquals(bytes.length - 3, Files.size(torn), "the tool does not change the file");

        // The same file treated as sealed is damaged.
        assertEquals(1, run("--sealed", torn.toString()));
        assertTrue(err().contains("corruption"), err());
        assertTrue(err().contains("sealed"), err());
    }

    @Test
    void damageInSyncedDataIsAnError() throws IOException {
        Path log = writeLog();
        byte[] bytes = Files.readAllBytes(log);
        bytes[25] ^= 0x04; // inside the first batch, which later sync points cover
        Path damaged = dir.resolve("000001.wal");
        Files.write(damaged, bytes);
        assertEquals(1, run(damaged.toString()));
        assertTrue(err().contains("corruption"), err());
        assertTrue(err().contains("at offset 14"), err());
    }

    @Test
    void aWrongFileNumberAndUsageErrors() throws IOException {
        Path log = writeLog();
        assertEquals(1, run("--sealed", "--file-number", "2", log.toString()));
        assertEquals(2, run());
        assertEquals(2, run("--bogus", log.toString()));
        Path renamed = dir.resolve("copy.wal");
        Files.copy(log, renamed);
        assertEquals(2, run(renamed.toString()));
        assertEquals(0, run("--file-number", "1", "--sealed", renamed.toString()), err());
    }
}
