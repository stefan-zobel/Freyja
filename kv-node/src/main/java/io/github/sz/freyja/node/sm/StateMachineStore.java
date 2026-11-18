package io.github.sz.freyja.node.sm;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import io.github.sz.freyja.common.kv.CommandResult;
import io.github.sz.freyja.lsm.CorruptionException;
import io.github.sz.freyja.lsm.EngineOptions;
import io.github.sz.freyja.lsm.UnsupportedFormatException;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.node.StateMachine;
import io.github.sz.freyja.node.store.Bootstrap;
import io.github.sz.freyja.node.store.GenerationPointer;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.LogEntry;
import io.github.sz.freyja.raft.SnapshotMeta;

/**
 * The generations of the state machine of a node (docs/spec/node.md, sections 2 and 5).
 *
 * <pre>
 * sm/CURRENT         which generation counts, and what it started from
 * sm/NNNNNNNNNN/     a generation: the directory of a storage engine
 * </pre>
 *
 * <p>The state machine is replaced as a whole when a snapshot of another node is installed.
 * The snapshot is received into the directory of a new generation, verified there, and
 * becomes the state machine when {@code CURRENT} names it. A generation that {@code CURRENT}
 * does not name is removed: at the next start, or when the store has no more use for it.
 *
 * <p>An instance is confined to one thread.
 */
public final class StateMachineStore implements StateMachine, Closeable {

    /** Subdirectory of the data directory that holds the generations. */
    public static final String DIRECTORY = "sm";

    /**
     * The settings of the state machine.
     *
     * @param engine               the settings of the storage engine of every generation
     * @param sessionTimeoutMillis see {@link KvStateMachine#open}
     */
    public record Options(EngineOptions engine, long sessionTimeoutMillis) {
    }

    private final StorageFileSystem fs;
    private final Path directory;
    private final Options options;
    private GenerationPointer current;
    private KvStateMachine machine;
    /** The highest generation number ever used here; the next generation gets the one after. */
    private long highestGeneration;
    /** Damage the engine of the state machine has reported; {@code null} if none. */
    private CorruptionException damage;
    private boolean verifying;

    private StateMachineStore(StorageFileSystem fs, Path directory, Options options, GenerationPointer current) {
        this.fs = fs;
        this.directory = directory;
        this.options = options;
        this.current = current;
        this.highestGeneration = current.generation();
    }

    /**
     * Prepares the state machine of a node that has never run: an empty first generation.
     *
     * @param config         the configuration the cluster starts with; empty for a node that
     *                       is to join a cluster
     * @param clusterVersion the cluster version the cluster starts with
     */
    public static void initialize(StorageFileSystem fs, Path dataDirectory, ClusterConfig config,
                                  int clusterVersion) throws IOException {
        Path directory = dataDirectory.resolve(DIRECTORY);
        fs.createDirectories(directory);
        new GenerationPointer(1, SnapshotMeta.none(config), clusterVersion).write(fs, directory);
    }

    /**
     * Makes {@code generation} the state machine of a node that is not running. This is the
     * step of an installation that a node repeats at its start if it crashed in the middle
     * of one. It has no effect if the generation counts already.
     */
    public static void adopt(StorageFileSystem fs, Path dataDirectory, long generation, SnapshotMeta origin,
                             int clusterVersion) throws IOException {
        Path directory = dataDirectory.resolve(DIRECTORY);
        GenerationPointer pointer = GenerationPointer.read(fs, directory);
        if (pointer == null || pointer.generation() != generation) {
            new GenerationPointer(generation, origin, clusterVersion).write(fs, directory);
        }
    }

    /**
     * Replaces the state machine of a node that is not running by an empty one, as the node
     * had it when its directory was prepared. This is how a damaged state machine is given
     * up; what it held comes back from the log or from another node.
     */
    public static void replaceByEmpty(StorageFileSystem fs, Path dataDirectory, Bootstrap bootstrap)
            throws IOException {
        Path directory = dataDirectory.resolve(DIRECTORY);
        fs.createDirectories(directory);
        GenerationPointer pointer = GenerationPointer.read(fs, directory);
        long highest = pointer == null ? 0 : pointer.generation();
        for (Path path : fs.list(directory)) {
            String name = path.getFileName().toString();
            if (name.matches("\\d{10}")) {
                highest = Math.max(highest, Long.parseLong(name));
            }
        }
        new GenerationPointer(highest + 1, SnapshotMeta.none(bootstrap.config()), bootstrap.clusterVersion())
                .write(fs, directory);
    }

    /**
     * Opens the generation that counts and removes every other one.
     *
     * <p>The store makes itself the damage listener of the engine settings: damage that the
     * engine finds later, in a read or in a scrub, is kept for {@link #damage}.
     *
     * @throws CorruptionException if {@code CURRENT} or the generation it names is damaged
     */
    public static StateMachineStore open(StorageFileSystem fs, Path dataDirectory, Options options)
            throws IOException {
        Path directory = dataDirectory.resolve(DIRECTORY);
        GenerationPointer pointer = GenerationPointer.read(fs, directory);
        if (pointer == null) {
            throw new CorruptionException(GenerationPointer.FILE_NAME, 0,
                    "the file that names the generation of the state machine is missing or damaged");
        }
        StateMachineStore store = new StateMachineStore(fs, directory, options, pointer);
        options.engine().damageListener(found -> {
            if (!store.verifying && store.damage == null) {
                store.damage = found;
            }
        });
        for (Path path : fs.list(directory)) {
            String name = path.getFileName().toString();
            if (name.matches("\\d{10}")) {
                long generation = Long.parseLong(name);
                store.highestGeneration = Math.max(store.highestGeneration, generation);
                if (generation != pointer.generation()) {
                    store.remove(generation);
                }
            }
        }
        store.machine = store.openMachine(pointer);
        return store;
    }

    private KvStateMachine openMachine(GenerationPointer pointer) throws IOException {
        return KvStateMachine.open(fs, directory(pointer.generation()), options.engine(),
                options.sessionTimeoutMillis(), pointer.origin().config(), pointer.clusterVersion());
    }

    // ------------------------------------------------------------------ the state machine

    /** The state machine of the generation that counts. It changes with an installation. */
    public KvStateMachine machine() {
        return machine;
    }

    @Override
    public long appliedIndex() {
        return machine.appliedIndex();
    }

    @Override
    public List<CommandResult> apply(List<LogEntry> entries) throws IOException {
        return machine.apply(entries);
    }

    /** What a snapshot of the state machine as it is now covers. */
    public SnapshotMeta meta() {
        return new SnapshotMeta(machine.appliedIndex(), machine.appliedTerm(), machine.config());
    }

    /**
     * Damage that the engine has found in the state machine since it was opened, or
     * {@code null}. A state machine with damage must not be used any further.
     */
    public CorruptionException damage() {
        return damage;
    }

    /** The number of the generation that counts. */
    public long generation() {
        return current.generation();
    }

    /** Makes everything applied so far durable. */
    public void flush() throws IOException {
        machine.engine().flush();
    }

    /**
     * Writes a consistent copy of the state machine into {@code target}, a directory that
     * does not exist yet. The copy holds everything applied so far.
     */
    public void checkpoint(Path target) throws IOException {
        machine.engine().checkpoint(target);
    }

    // ------------------------------------------------------------------ generations

    /** The directory of a generation. */
    public Path directory(long generation) {
        return directory.resolve(GenerationPointer.directoryName(generation));
    }

    /** Creates the empty directory of a new generation, to receive a snapshot into. */
    public long stage() throws IOException {
        long generation = ++highestGeneration;
        fs.createDirectories(directory(generation));
        return generation;
    }

    /**
     * Checks a received generation by its own checksums: it must open as a state machine,
     * hold exactly what {@code meta} says, and every block of every table must be intact.
     * Nothing of a damaged snapshot ever becomes the state machine.
     *
     * <p>A state machine that never applied a CONFIG entry stores no configuration, and one
     * whose cluster version was never raised stores no version. For those the word of the
     * sender is all there is; it becomes the origin of the generation.
     */
    public boolean verify(long generation, SnapshotMeta meta, int clusterVersion) throws IOException {
        verifying = true;
        try (KvStateMachine staged = KvStateMachine.open(fs, directory(generation), options.engine(),
                options.sessionTimeoutMillis(), meta.config(), clusterVersion)) {
            return staged.appliedIndex() == meta.lastIncludedIndex()
                    && staged.appliedTerm() == meta.lastIncludedTerm()
                    && staged.config().equals(meta.config())
                    && staged.clusterVersion() == clusterVersion
                    && staged.engine().scrub() == 0;
        } catch (CorruptionException | UnsupportedFormatException e) {
            return false;
        } finally {
            verifying = false;
        }
    }

    /**
     * Makes a verified generation the state machine. The old generation is closed and
     * removed. After a crash, {@code CURRENT} names the old or the new generation.
     */
    public void install(long generation, SnapshotMeta origin, int clusterVersion) throws IOException {
        GenerationPointer next = new GenerationPointer(generation, origin, clusterVersion);
        long old = current.generation();
        machine.close();
        machine = null;
        next.write(fs, directory);
        current = next;
        machine = openMachine(next);
        remove(old);
    }

    /** Removes a generation that does not count: its files and its directory. */
    public void discard(long generation) throws IOException {
        if (generation == current.generation()) {
            throw new IllegalArgumentException("generation " + generation + " is the state machine");
        }
        remove(generation);
    }

    private void remove(long generation) throws IOException {
        removeTree(fs, directory(generation));
    }

    /** Removes a directory with everything in it. */
    public static void removeTree(StorageFileSystem fs, Path directory) throws IOException {
        if (!fs.exists(directory)) {
            return;
        }
        for (Path path : fs.list(directory)) {
            if (isDirectory(fs, path)) {
                removeTree(fs, path);
            } else {
                fs.delete(path);
            }
        }
        fs.deleteDirectory(directory);
    }

    private static boolean isDirectory(StorageFileSystem fs, Path path) throws IOException {
        try {
            fs.list(path);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        if (machine != null) {
            machine.close();
            machine = null;
        }
    }
}
