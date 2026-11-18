package io.github.sz.freyja.node.sm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.common.kv.Command;
import io.github.sz.freyja.common.kv.Command.Put;
import io.github.sz.freyja.common.kv.CommandCodec;
import io.github.sz.freyja.lsm.CorruptionException;
import io.github.sz.freyja.lsm.EngineOptions;
import io.github.sz.freyja.lsm.fs.MemoryFileSystem;
import io.github.sz.freyja.lsm.fs.ReadableFile;
import io.github.sz.freyja.lsm.fs.WritableFile;
import io.github.sz.freyja.node.store.GenerationPointer;
import io.github.sz.freyja.node.store.InstallIntent;
import io.github.sz.freyja.node.store.SnapshotMetaFile;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.ConfigChange;
import io.github.sz.freyja.raft.EntryType;
import io.github.sz.freyja.raft.LogEntry;
import io.github.sz.freyja.raft.SnapshotMeta;

/** The generations of the state machine and the small files around an installation. */
class StateMachineStoreTest {

    private static final Path DATA = Path.of("data");
    private static final Path SM = DATA.resolve(StateMachineStore.DIRECTORY);
    private static final ClusterConfig THREE = ClusterConfig.ofVoters(0, 1, 2);

    private static StateMachineStore.Options options() {
        return new StateMachineStore.Options(new EngineOptions().deterministic(true).reserveSize(0).minFreeSpace(0)
                .memTableSize(16 * 1024).blockCacheCapacity(1024 * 1024), 60_000);
    }

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /** Applies a session and {@code count} writes, as entries of term 2. */
    private static void fill(StateMachineStore store, int count) throws IOException {
        long index = store.appliedIndex();
        store.apply(List.of(new LogEntry(++index, 2, EntryType.COMMAND,
                CommandCodec.encode(10, new Command.RegisterSession()))));
        long session = index;
        for (int i = 1; i <= count; i++) {
            store.apply(List.of(new LogEntry(++index, 2, EntryType.COMMAND, CommandCodec.encode(10 + i,
                    new Command.Write(session, i, List.of(new Put(b("key-" + i), new byte[400])))))));
        }
    }

    /** Copies a checkpoint file by file, as a transfer does. */
    private static void copy(MemoryFileSystem fs, Path from, Path to) throws IOException {
        for (Path path : fs.list(from)) {
            byte[] bytes;
            try (ReadableFile in = fs.open(path)) {
                bytes = new byte[(int) in.size()];
                in.readFully(0, bytes, 0, bytes.length);
            }
            try (WritableFile out = fs.create(to.resolve(path.getFileName()))) {
                out.append(bytes, 0, bytes.length);
                out.sync();
            }
        }
        fs.syncDirectory(to);
    }

    private static List<String> names(MemoryFileSystem fs, Path directory) throws IOException {
        return fs.list(directory).stream().map(p -> p.getFileName().toString()).toList();
    }

    @Test
    void aReceivedGenerationIsVerifiedAndBecomesTheStateMachine() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        fs.createDirectories(DATA);
        ClusterConfig grown = THREE.with(new ConfigChange.AddLearner(4));
        // The source: another node, here another directory of the same file system.
        Path sourceData = Path.of("source");
        fs.createDirectories(sourceData);
        StateMachineStore.initialize(fs, sourceData, THREE, 3);
        Path checkpoint = Path.of("checkpoint");
        SnapshotMeta meta;
        long digest;
        try (StateMachineStore source = StateMachineStore.open(fs, sourceData, options())) {
            source.apply(List.of(new LogEntry(1, 2, EntryType.CONFIG, grown.encode())));
            fill(source, 60);
            meta = source.meta();
            digest = source.machine().digest();
            source.checkpoint(checkpoint);
            fill(source, 5); // the checkpoint does not move with the source
        }
        assertEquals(new SnapshotMeta(62, 2, grown), meta);

        StateMachineStore.initialize(fs, DATA, ClusterConfig.EMPTY, 1);
        try (StateMachineStore store = StateMachineStore.open(fs, DATA, options())) {
            assertEquals(1, store.generation());
            assertEquals(new SnapshotMeta(0, 0, ClusterConfig.EMPTY), store.meta());
            fill(store, 3);

            long generation = store.stage();
            assertEquals(2, generation);
            copy(fs, checkpoint, store.directory(generation));
            assertFalse(store.verify(generation, new SnapshotMeta(61, 2, grown), 3), "another index");
            assertFalse(store.verify(generation, new SnapshotMeta(62, 1, grown), 3), "another term");
            assertFalse(store.verify(generation, new SnapshotMeta(62, 2, THREE), 3), "another configuration");
            assertTrue(store.verify(generation, meta, 3));
            assertEquals(4, store.appliedIndex(), "verifying changes nothing");
            assertThrows(IllegalArgumentException.class, () -> store.discard(1));

            store.install(generation, meta, 3);
            assertEquals(2, store.generation());
            assertEquals(meta, store.meta());
            assertEquals(digest, store.machine().digest());
            assertEquals(3, store.machine().clusterVersion());
            assertEquals(List.of("0000000002", "CURRENT"), names(fs, SM));

            // A generation that is not wanted is removed, and its number is not used again.
            long next = store.stage();
            assertEquals(3, next);
            copy(fs, checkpoint, store.directory(next));
            store.discard(next);
            assertEquals(List.of("0000000002", "CURRENT"), names(fs, SM));
            assertEquals(4, store.stage());
        }
        // What a stopped node left staged is removed at the next start.
        assertEquals(List.of("0000000002", "0000000004", "CURRENT"), names(fs, SM));
        try (StateMachineStore store = StateMachineStore.open(fs, DATA, options())) {
            assertEquals(List.of("0000000002", "CURRENT"), names(fs, SM));
            assertEquals(meta, store.meta());
            assertEquals(grown, store.machine().config(), "the configuration of the origin");
            assertEquals(5, store.stage(), "numbers of generations only rise");
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void aDamagedTableInAReceivedGenerationIsFound() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        fs.createDirectories(DATA);
        StateMachineStore.initialize(fs, DATA, THREE, 1);
        try (StateMachineStore store = StateMachineStore.open(fs, DATA, options())) {
            fill(store, 80);
            SnapshotMeta meta = store.meta();
            Path checkpoint = Path.of("checkpoint");
            store.checkpoint(checkpoint);
            int found = 0;
            for (Path table : fs.list(checkpoint)) {
                long size = fs.size(table);
                if (!table.getFileName().toString().endsWith(".sst") || size < 12_000) {
                    continue;
                }
                long generation = store.stage();
                copy(fs, checkpoint, store.directory(generation));
                Path copy = store.directory(generation).resolve(table.getFileName());
                byte[] one = fs.peek(copy, 6_000, 1);
                one[0] ^= 0x20;
                fs.overwrite(copy, 6_000, one);
                assertFalse(store.verify(generation, meta, 1), "damage in " + table.getFileName());
                store.discard(generation);
                found++;
            }
            assertTrue(found > 0, "a table with several blocks");
            // A missing table.
            long generation = store.stage();
            copy(fs, checkpoint, store.directory(generation));
            for (Path path : fs.list(store.directory(generation))) {
                if (path.getFileName().toString().endsWith(".sst")) {
                    fs.delete(path);
                    break;
                }
            }
            assertFalse(store.verify(generation, meta, 1));
        }
    }

    @Test
    void withoutTheFileThatNamesTheGenerationTheStateMachineDoesNotOpen() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        fs.createDirectories(DATA);
        StateMachineStore.initialize(fs, DATA, THREE, 1);
        StateMachineStore.open(fs, DATA, options()).close();
        Path current = SM.resolve(GenerationPointer.FILE_NAME);
        byte[] one = fs.peek(current, 6, 1);
        one[0] ^= 1;
        fs.overwrite(current, 6, one);
        assertNull(GenerationPointer.read(fs, SM));
        assertThrows(CorruptionException.class, () -> StateMachineStore.open(fs, DATA, options()));
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void theSmallFilesOfAnInstallationSurviveWritingAndRejectDamage() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        fs.createDirectories(DATA);
        SnapshotMeta meta = new SnapshotMeta(77, 5, THREE.with(new ConfigChange.AddLearner(9)));

        assertNull(SnapshotMetaFile.read(fs, DATA));
        SnapshotMetaFile.write(fs, DATA, meta);
        assertEquals(meta, SnapshotMetaFile.read(fs, DATA));
        SnapshotMetaFile.write(fs, DATA, SnapshotMeta.none(ClusterConfig.EMPTY));
        assertEquals(SnapshotMeta.none(ClusterConfig.EMPTY), SnapshotMetaFile.read(fs, DATA));

        assertNull(InstallIntent.read(fs, DATA));
        for (int flags = 0; flags < 4; flags++) {
            InstallIntent intent = new InstallIntent(meta, 12, 3, (flags & 1) != 0, (flags & 2) != 0);
            intent.write(fs, DATA);
            assertEquals(intent, InstallIntent.read(fs, DATA));
        }
        Path file = DATA.resolve(InstallIntent.FILE_NAME);
        byte[] one = fs.peek(file, 9, 1);
        one[0] ^= 1;
        fs.overwrite(file, 9, one);
        assertThrows(IOException.class, () -> InstallIntent.read(fs, DATA), "how far it went is unknown");
        InstallIntent.clear(fs, DATA);
        assertNull(InstallIntent.read(fs, DATA));
        InstallIntent.clear(fs, DATA);

        // The content of one small file is not valid in another.
        new GenerationPointer(4, meta, 2).write(fs, DATA);
        assertEquals(new GenerationPointer(4, meta, 2), GenerationPointer.read(fs, DATA));
        fs.rename(DATA.resolve(GenerationPointer.FILE_NAME), DATA.resolve(SnapshotMetaFile.FILE_NAME));
        assertNull(SnapshotMetaFile.read(fs, DATA));
    }
}
