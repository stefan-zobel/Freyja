package io.github.sz.freyja.lsm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.github.sz.freyja.lsm.fs.RealFileSystem;
import io.github.sz.freyja.lsm.manifest.CurrentFile;
import io.github.sz.freyja.lsm.manifest.FileNames;

/**
 * File-handle leaks (docs/spec/verification.md, section 2.3): after many flush, compaction,
 * and checkpoint cycles on the real file system and a close, only the files the manifest
 * refers to remain, and the directory can be deleted. On Windows an open handle makes the
 * deletion fail, so this test must pass there.
 */
class HandleLeakTest {

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static List<String> names(Path dir) throws IOException {
        try (Stream<Path> stream = Files.list(dir)) {
            return stream.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }

    private static void deleteTree(Path root) throws IOException {
        List<Path> paths;
        try (Stream<Path> stream = Files.walk(root)) {
            paths = new ArrayList<>(stream.sorted(Comparator.reverseOrder()).toList());
        }
        for (Path path : paths) {
            Files.delete(path); // fails if a handle to the file is still open
        }
    }

    private void runCycles(RealFileSystem fs, Path dir, Path checkpoints, EngineOptions options)
            throws IOException {
        Random random = new Random(11);
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            for (int cycle = 0; cycle < 40; cycle++) {
                for (int i = 0; i < 60; i++) {
                    byte[] key = b("key-" + random.nextInt(300));
                    if (random.nextInt(6) == 0) {
                        engine.write(new WriteBatch().delete(key), false);
                    } else {
                        engine.write(new WriteBatch().put(key, new byte[random.nextInt(400)]), false);
                    }
                }
                engine.flush();
                if (cycle % 3 == 0) {
                    engine.compact();
                }
                // Readers that outlive a compaction keep its inputs open for a while.
                try (Snapshot snapshot = engine.snapshot();
                     ScanIterator scan = engine.scan(null, null, snapshot)) {
                    for (int i = 0; i < 20 && scan.valid(); i++) {
                        scan.next();
                    }
                    engine.compact();
                    while (scan.valid()) {
                        scan.next();
                    }
                }
                if (cycle % 10 == 5) {
                    Path copy = checkpoints.resolve("copy-" + cycle);
                    engine.checkpoint(copy);
                    try (LsmEngine other = LsmEngine.open(fs, copy, options)) {
                        other.put(b("in-copy"), b("x"));
                        other.flush();
                        other.compact();
                    }
                }
                engine.scrub();
            }
            assertEquals(0, engine.openScanCount());
            assertEquals(0, engine.blockCache().pinnedCount());
        }
    }

    private void checkOnlyLiveFilesRemain(RealFileSystem fs, Path dir, EngineOptions options)
            throws IOException {
        EngineVerifier.Report report = EngineVerifier.verify(fs, dir, options.levelCount());
        assertTrue(report.intact(), report.problems().toString());
        List<String> names = names(dir);
        long tables = names.stream().filter(n -> n.endsWith(".sst")).count();
        long logs = names.stream().filter(n -> n.endsWith(".wal")).count();
        long manifests = names.stream().filter(n -> n.startsWith("MANIFEST-")).count();
        assertEquals(report.tables(), tables, "table files that the manifest does not list: " + names);
        assertEquals(1, logs, names.toString());
        assertEquals(1, manifests, names.toString());
        assertTrue(names.contains(FileNames.manifestName(CurrentFile.read(fs, dir))), names.toString());
        assertEquals(tables + 4, names.size(), "tables, one log, one manifest, CURRENT, LOCK: " + names);
        assertFalse(names.stream().anyMatch(n -> n.endsWith(".tmp")), names.toString());
    }

    @Test
    void afterManyCyclesOnlyLiveFilesRemainAndTheDirectoryCanBeDeleted(@TempDir Path temp)
            throws IOException {
        RealFileSystem fs = new RealFileSystem();
        Path dir = temp.resolve("db");
        Path checkpoints = temp.resolve("checkpoints");
        Files.createDirectories(checkpoints);
        EngineOptions options = new EngineOptions()
                .deterministic(true)
                .reserveSize(0)
                .minFreeSpace(0)
                .blockCacheCapacity(128 * 1024)
                .memTableSize(8 * 1024)
                .level0CompactionTrigger(2)
                .level1TargetSize(32 * 1024)
                .targetFileSize(8 * 1024)
                .levelCount(4)
                .maxManifestSize(2000)
                .tableMetadataBudget(2000);

        runCycles(fs, dir, checkpoints, options);
        checkOnlyLiveFilesRemain(fs, dir, options);
        try (Stream<Path> copies = Files.list(checkpoints)) {
            for (Path copy : copies.toList()) {
                checkOnlyLiveFilesRemain(fs, copy, options);
            }
        }
        deleteTree(temp.resolve("checkpoints"));
        deleteTree(dir);
        assertFalse(Files.exists(dir));
    }

    @Test
    void theSameHoldsWithABackgroundThread(@TempDir Path temp) throws Exception {
        RealFileSystem fs = new RealFileSystem();
        Path dir = temp.resolve("db");
        EngineOptions options = new EngineOptions()
                .reserveSize(64 * 1024)
                .minFreeSpace(0)
                .blockCacheCapacity(128 * 1024)
                .memTableSize(8 * 1024)
                .level0CompactionTrigger(2)
                .level1TargetSize(32 * 1024)
                .targetFileSize(8 * 1024)
                .levelCount(4)
                .maxManifestSize(2000)
                .scrubPeriodMillis(5);
        Random random = new Random(12);
        try (LsmEngine engine = LsmEngine.open(fs, dir, options)) {
            for (int i = 0; i < 4000; i++) {
                engine.write(new WriteBatch().put(b("key-" + random.nextInt(400)), new byte[random.nextInt(300)]), false);
                if (i % 500 == 0) {
                    try (ScanIterator scan = engine.scan(null, null)) {
                        while (scan.valid()) {
                            scan.next();
                        }
                    }
                }
            }
            engine.flush();
        }
        List<String> names = names(dir);
        assertTrue(names.contains(FileNames.RESERVE), names.toString());
        assertEquals(64 * 1024, Files.size(dir.resolve(FileNames.RESERVE)));
        assertTrue(EngineVerifier.verify(fs, dir, 4).intact());
        deleteTree(dir);
        assertFalse(Files.exists(dir));
    }
}
