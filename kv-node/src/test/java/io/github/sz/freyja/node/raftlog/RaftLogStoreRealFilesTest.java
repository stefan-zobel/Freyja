package io.github.sz.freyja.node.raftlog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.github.sz.freyja.lsm.fs.RealFileSystem;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.EntryType;
import io.github.sz.freyja.raft.LogEntry;

/**
 * The log store on the real file system: segments are read while later ones are written,
 * deleted while the store is open, and after a close no handle is left. On Windows an open
 * handle makes a deletion fail, so this test must pass there.
 */
class RaftLogStoreRealFilesTest {

    private static List<LogEntry> batch(long from, int count, long term) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            entries.add(new LogEntry(from + i, term, EntryType.COMMAND, new byte[200]));
        }
        return entries;
    }

    @Test
    void segmentsAreWrittenReadDeletedAndReleased(@TempDir Path temp) throws IOException {
        RealFileSystem fs = new RealFileSystem();
        Path directory = temp.resolve("raft");
        ClusterConfig config = ClusterConfig.ofVoters(0, 1, 2);
        try (RaftLogStore store = RaftLogStore.open(fs, directory, 4096, 0, config)) {
            for (int i = 0; i < 50; i++) {
                store.append(batch(store.lastIndex() + 1, 4, 1));
                store.sync();
                // Read from sealed segments and from the one being written.
                assertEquals(4, store.entries(store.lastIndex() - 3, store.lastIndex() + 1, 1 << 20).size());
                assertEquals(1, store.entries(1 + i, 2 + i, 1 << 20).size());
            }
            int before = store.segmentCount();
            assertTrue(before > 5);
            store.truncatePrefix(150);
            assertTrue(store.segmentCount() < before);
            assertEquals(200, store.lastIndex());
            assertEquals(50, store.entries(151, 201, 1 << 20).size());
            store.truncateSuffix(190);
            store.append(batch(190, 3, 2));
            store.sync();
        }
        try (RaftLogStore store = RaftLogStore.open(fs, directory, 4096, 150, config)) {
            assertEquals(-1, store.firstDamagedIndex());
            assertEquals(192, store.lastIndex());
            assertEquals(2, store.termAt(190));
            assertEquals(1, store.termAt(189));
        }
        // Every handle is closed: the directory can be deleted.
        List<Path> paths;
        try (Stream<Path> stream = Files.walk(directory)) {
            paths = stream.sorted(Comparator.reverseOrder()).toList();
        }
        for (Path path : paths) {
            Files.delete(path);
        }
        assertFalse(Files.exists(directory));
    }
}
