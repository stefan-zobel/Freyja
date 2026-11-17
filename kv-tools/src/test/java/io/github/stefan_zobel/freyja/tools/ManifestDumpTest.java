package io.github.stefan_zobel.freyja.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

import io.github.stefan_zobel.freyja.lsm.EngineOptions;
import io.github.stefan_zobel.freyja.lsm.LsmEngine;
import io.github.stefan_zobel.freyja.lsm.fs.RealFileSystem;
import io.github.stefan_zobel.freyja.lsm.manifest.CurrentFile;
import io.github.stefan_zobel.freyja.lsm.manifest.FileNames;

/** Runs the manifest dump tool against a manifest written by the engine. */
class ManifestDumpTest {

    @TempDir
    Path dir;

    private final RealFileSystem fs = new RealFileSystem();
    private final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();

    private int run(String... args) {
        outBytes.reset();
        errBytes.reset();
        return ManifestDump.run(args, fs,
                new PrintStream(outBytes, true, StandardCharsets.UTF_8),
                new PrintStream(errBytes, true, StandardCharsets.UTF_8));
    }

    private String out() {
        return outBytes.toString(StandardCharsets.UTF_8);
    }

    private String err() {
        return errBytes.toString(StandardCharsets.UTF_8);
    }

    /** Runs an engine through two flushes and returns its manifest. */
    private Path writeManifest() throws IOException {
        Path engineDir = dir.resolve("db");
        try (LsmEngine engine = LsmEngine.open(fs, engineDir, new EngineOptions().blockCacheCapacity(0))) {
            engine.put(new byte[] {0x61}, new byte[] {1});
            engine.put(new byte[] {0x62}, new byte[] {2});
            engine.flush();
            engine.delete(new byte[] {0x61});
            engine.flush();
        }
        return FileNames.manifest(engineDir, CurrentFile.read(fs, engineDir));
    }

    @Test
    void printsEveryEditAndTheResultingVersion() throws IOException {
        Path manifest = writeManifest();
        assertEquals("MANIFEST-000002", manifest.getFileName().toString());
        assertEquals(0, run(manifest.toString()), err());
        String text = out();
        assertTrue(text.contains("file number:  2"), text);
        // The first edit is the snapshot written when the engine opened.
        assertTrue(text.contains("edit 1 at 14:"), text);
        assertTrue(text.contains("  min WAL number:   1"), text);
        assertTrue(text.contains("  add level 0: file 4, "), text);
        assertTrue(text.contains("keys [61 @1 VALUE .. 62 @2 VALUE], sequences 1 to 2"), text);
        assertTrue(text.contains("  add level 0: file 6, "), text);
        assertTrue(text.contains("keys [61 @3 TOMBSTONE .. 61 @3 TOMBSTONE], sequences 3 to 3"), text);
        assertTrue(text.contains("edits:        3"), text);
        assertTrue(text.contains("end:          clean"), text);
        assertTrue(text.contains("  next file number:  7"), text);
        assertTrue(text.contains("  last sequence:     3"), text);
        assertTrue(text.contains("  min WAL number:    5"), text);
        assertTrue(text.contains("  level 0: 2 files"), text);
        assertTrue(text.indexOf("    file 6,") < text.indexOf("    file 4,"), "newest file first");
    }

    @Test
    void aTornTailIsReportedAndDamageIsAnError() throws IOException {
        Path manifest = writeManifest();
        byte[] bytes = Files.readAllBytes(manifest);

        Path torn = dir.resolve("MANIFEST-000002");
        Files.write(torn, Arrays.copyOf(bytes, bytes.length - 2));
        assertEquals(0, run(torn.toString()), err());
        assertTrue(out().contains("edits:        2"), out());
        assertTrue(out().contains("end:          torn tail at "), out());

        byte[] damaged = bytes.clone();
        damaged[22] ^= 0x01; // inside the first edit
        Files.write(torn, damaged);
        assertEquals(1, run(torn.toString()));
        assertTrue(err().contains("corruption"), err());
    }

    @Test
    void wrongKindWrongNumberAndUsageErrors() throws IOException {
        Path manifest = writeManifest();
        // Under another file number no checksum matches. For the last file of its kind that
        // is a torn tail from the first byte on: the tool reports it and finds no edit.
        assertEquals(0, run("--file-number", "3", manifest.toString()), err());
        assertTrue(out().contains("edits:        0"), out());
        assertTrue(out().contains("end:          torn tail at 0 "), out());

        // A write-ahead log is not a manifest.
        Path log = Files.list(manifest.getParent())
                .filter(p -> p.getFileName().toString().endsWith(".wal")).findFirst().orElseThrow();
        assertEquals(2, run(log.toString()), "the name is no manifest name");
        long logNumber = FileNames.parse(log).number();
        assertEquals(1, run("--file-number", String.valueOf(logNumber), log.toString()));
        assertTrue(err().contains("MANIFEST"), err());

        assertEquals(2, run());
        assertEquals(2, run("--levels", "1", manifest.toString()));
        assertEquals(2, run("--bogus", manifest.toString()));
        assertEquals(0, run("--levels", "3", manifest.toString()), err());
    }
}
