package io.github.stefan_zobel.freyja.lsm;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import io.github.stefan_zobel.freyja.common.ByteComparator;
import io.github.stefan_zobel.freyja.common.DecodeException;
import io.github.stefan_zobel.freyja.lsm.fs.ReadableFile;
import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;
import io.github.stefan_zobel.freyja.lsm.log.FileKind;
import io.github.stefan_zobel.freyja.lsm.log.LogReader;
import io.github.stefan_zobel.freyja.lsm.log.LogWriter;
import io.github.stefan_zobel.freyja.lsm.manifest.CurrentFile;
import io.github.stefan_zobel.freyja.lsm.manifest.FileMeta;
import io.github.stefan_zobel.freyja.lsm.manifest.FileNames;
import io.github.stefan_zobel.freyja.lsm.manifest.Version;
import io.github.stefan_zobel.freyja.lsm.manifest.VersionEdit;
import io.github.stefan_zobel.freyja.lsm.table.BlockCache;
import io.github.stefan_zobel.freyja.lsm.table.InternalIterator;
import io.github.stefan_zobel.freyja.lsm.table.InternalKey;
import io.github.stefan_zobel.freyja.lsm.table.Lookup;
import io.github.stefan_zobel.freyja.lsm.table.SSTableReader;
import io.github.stefan_zobel.freyja.lsm.table.SSTableWriter;
import io.github.stefan_zobel.freyja.lsm.table.TableMetadataBudget;

/**
 * The storage engine: a write-ahead log, MemTables, and table files in levels, tied together
 * by a manifest (docs/spec/storage-engine.md).
 *
 * <p><b>Writes.</b> One writer at a time appends to the log and the MemTable. Writers that
 * arrive meanwhile queue up and are committed as a group with a single sync. A write with
 * {@code sync = true} returns only after its log record is durable.
 *
 * <p><b>Flush.</b> A full MemTable is frozen together with its log, and a new MemTable with a
 * new log takes over. The frozen MemTable is written to a table file in level 0; then its log
 * is deleted. There is one log per MemTable.
 *
 * <p><b>Compaction</b> merges the files of a level into the next level and drops versions
 * that no reader can see any more. A table file is deleted when no version and no reader
 * refers to it.
 *
 * <p><b>Background work.</b> In normal mode one background thread flushes and compacts. In
 * deterministic mode there is no thread: a flush runs inside the write that fills the
 * MemTable, and compaction runs when the caller asks for it or when a write would otherwise
 * stall.
 *
 * <p><b>Backpressure.</b> Writes are delayed when level 0 grows long, and they stall when it
 * grows too long or when too many MemTables wait for their flush. A stalled write fails with
 * {@link WriteStalledException} when its time is over.
 *
 * <p><b>Reads</b> never block. They look at the MemTables and at the table files, newest
 * first, and see every write that has returned. A {@link Snapshot} fixes the sequence number
 * a read uses; a {@link ScanIterator} walks a key range at one sequence number.
 *
 * <p><b>Failure.</b> If a write to the log, to a flushed table, or to the manifest fails, the
 * instance enters a failed state and rejects all further writes with
 * {@link EngineFailedException}. Damage found while opening prevents opening; the engine
 * never opens with data loss.
 *
 * <p>Safe for concurrent use. The engine must not be used while or after it is closed, and
 * every scan must be closed before the engine is.
 */
public final class LsmEngine implements Closeable {

    /** A queued request for the exclusive right to write to the log and the MemTable. */
    private static final class Pending {
        final WriteBatch batch; // null for a request that is not a write
        final boolean sync;
        final Condition turn;
        boolean done;
        IOException error;

        Pending(WriteBatch batch, boolean sync, Condition turn) {
            this.batch = batch;
            this.sync = sync;
            this.turn = turn;
        }
    }

    /**
     * A full MemTable that waits for its flush.
     *
     * @param mem          the MemTable; it receives no more writes
     * @param walNumber    the log that holds its contents; sealed
     * @param lastSequence the last sequence number it holds
     */
    private record Frozen(MemTable mem, long walNumber, long lastSequence) {
    }

    /**
     * Everything a read looks at, replaced as a whole.
     *
     * <p>A read state holds a reference to the reader of every table file of its version.
     * It is created with one pin, owned by the engine while the state is current. A read
     * takes another pin for its duration. When the last pin is gone, the references to the
     * readers are released, and a table that has dropped out of the current version is
     * closed and deleted.
     */
    private static final class ReadState {
        private final MemTable mem;
        private final long memWalNumber;
        private final List<Frozen> frozen; // oldest first
        private final Version version;
        private final Map<Long, SSTableReader> readers;
        private final AtomicInteger pins = new AtomicInteger(1);

        /**
         * @param mem          the MemTable that receives writes
         * @param memWalNumber the log of {@code mem}
         * @param frozen       the MemTables that wait for their flush, oldest first
         * @param version      the live table files
         * @param live         the readers of at least all files of {@code version}
         */
        ReadState(MemTable mem, long memWalNumber, List<Frozen> frozen, Version version,
                  Map<Long, SSTableReader> live) {
            this.mem = mem;
            this.memWalNumber = memWalNumber;
            this.frozen = List.copyOf(frozen);
            this.version = version;
            Map<Long, SSTableReader> mine = new HashMap<>();
            for (FileMeta file : version.allFiles()) {
                SSTableReader reader = live.get(file.number());
                reader.retain();
                mine.put(file.number(), reader);
            }
            this.readers = mine;
        }

        MemTable mem() {
            return mem;
        }

        long memWalNumber() {
            return memWalNumber;
        }

        List<Frozen> frozen() {
            return frozen;
        }

        Version version() {
            return version;
        }

        SSTableReader reader(long fileNumber) {
            return readers.get(fileNumber);
        }

        /** Takes a pin unless the state has been released completely. */
        boolean tryPin() {
            while (true) {
                int current = pins.get();
                if (current == 0) {
                    return false;
                }
                if (pins.compareAndSet(current, current + 1)) {
                    return true;
                }
            }
        }

        /** Gives a pin back. With the last pin the references to the readers are released. */
        void unpin() throws IOException {
            if (pins.decrementAndGet() != 0) {
                return;
            }
            IOException first = null;
            for (SSTableReader reader : readers.values()) {
                try {
                    reader.release();
                } catch (IOException e) {
                    if (first == null) {
                        first = e;
                    } else {
                        first.addSuppressed(e);
                    }
                }
            }
            if (first != null) {
                throw first;
            }
        }
    }

    /** What one compaction reads and where it writes. */
    private record CompactionPlan(int level, List<FileMeta> inputs, List<FileMeta> nextLevelInputs) {
        int outputLevel() {
            return level + 1;
        }
    }

    @FunctionalInterface
    private interface Action {
        void run() throws IOException;
    }

    private final StorageFileSystem fs;
    private final Path dir;
    private final EngineOptions options;

    // Locks, in the order in which they may be nested:
    //   flushLock or compactionLock  ->  stateLock  ->  lock
    // The request at the head of the write queue does its I/O without holding `lock`.

    /** Guards the write queue, the failure and close flags, and the conditions below. */
    private final ReentrantLock lock = new ReentrantLock();
    private final ArrayDeque<Pending> queue = new ArrayDeque<>();
    private final Condition backgroundSignal = lock.newCondition();
    private final Condition progress = lock.newCondition(); // a flush or compaction finished
    private boolean backgroundWake;              // guarded by lock
    private volatile IOException failure;        // written with lock held
    private volatile boolean closing;            // a close has been requested
    private volatile boolean closed;             // written with lock held

    /** Guards the manifest writer and every replacement of the read state. */
    private final ReentrantLock stateLock = new ReentrantLock();
    /** Held by the one flush that runs at a time. */
    private final ReentrantLock flushLock = new ReentrantLock();
    /** Held by the one compaction that runs at a time. */
    private final ReentrantLock compactionLock = new ReentrantLock();

    // Used only by the request at the head of the write queue, and during open and close.
    private LogWriter wal;
    private long walNumber;

    // Used only with stateLock held, and during open and close.
    private LogWriter manifest;
    private long manifestNumber;

    private final AtomicLong nextFileNumber = new AtomicLong();

    /**
     * The readers of the table files of the current version. Each is held with one reference
     * that is released when its file leaves the current version or the engine is closed.
     */
    private final Map<Long, SSTableReader> tables = new ConcurrentHashMap<>();
    private BlockCache cache;
    private TableMetadataBudget metadataBudget;
    private RateLimiter compactionLimiter;
    private Closeable directoryLock;
    private Thread backgroundThread;

    /** Number of writers that are being delayed or stalled right now. */
    private final AtomicInteger heldBackWriters = new AtomicInteger();

    private volatile ReadState state;

    /** Set when the engine is closed, so that reads fail instead of using released tables. */
    private volatile boolean shutDown;

    /** Live snapshots: sequence number to number of snapshots at it. */
    private final TreeMap<Long, Integer> snapshots = new TreeMap<>();
    private final AtomicInteger openScans = new AtomicInteger();

    /** Sequence number of the last write that is visible to readers. */
    private volatile long lastSequence;

    /** The problem that ended the last background compaction, or {@code null}. */
    private volatile IOException lastCompactionProblem;

    private LsmEngine(StorageFileSystem fs, Path dir, EngineOptions options) {
        this.fs = fs;
        this.dir = dir;
        this.options = new EngineOptions(options);
    }

    /**
     * Opens the engine in {@code dir}, creating the directory if needed.
     *
     * @throws CorruptionException        if a file is damaged or missing
     * @throws UnsupportedFormatException if a file uses a format this release does not know
     * @throws IOException                if another owner holds the directory, or on I/O errors
     */
    public static LsmEngine open(StorageFileSystem fs, Path dir, EngineOptions options)
            throws IOException {
        LsmEngine engine = new LsmEngine(fs, dir, options);
        if (engine.options.level0StallTrigger() < engine.options.level0CompactionTrigger()) {
            throw new IllegalArgumentException("writes would stall at "
                    + engine.options.level0StallTrigger() + " level-0 files, before compaction starts at "
                    + engine.options.level0CompactionTrigger());
        }
        fs.createDirectories(dir);
        engine.directoryLock = fs.lock(dir.resolve(FileNames.LOCK));
        try {
            engine.cache = new BlockCache(engine.options.blockCacheCapacity());
            engine.metadataBudget = new TableMetadataBudget(engine.options.tableMetadataBudget());
            // Without a background thread nothing may wait for time to pass.
            engine.compactionLimiter = new RateLimiter(
                    engine.options.deterministic() ? 0 : engine.options.compactionBytesPerSecond(),
                    engine.options.clock(), RateLimiter.PARK, () -> engine.heldBackWriters.get() > 0);
            engine.recover();
        } catch (Throwable t) {
            engine.releaseResources(t);
            throw t;
        }
        if (!engine.options.deterministic()) {
            engine.backgroundThread = Thread.ofPlatform()
                    .daemon(true)
                    .name("freyja-lsm-background")
                    .start(engine::backgroundLoop);
            engine.wakeBackground(); // level 0 may be due for compaction already
        }
        return engine;
    }

    // ---- recovery --------------------------------------------------------------------------

    private void recover() throws IOException {
        TreeMap<Long, Path> logs = new TreeMap<>();
        long maxNumber = 0;
        boolean hasData = false;
        for (Path path : fs.list(dir)) {
            FileNames.Numbered file = FileNames.parse(path);
            if (file == null) {
                continue;
            }
            maxNumber = Math.max(maxNumber, file.number());
            if (file.kind() == FileNames.Kind.WAL) {
                logs.put(file.number(), path);
                hasData = true;
            } else if (file.kind() == FileNames.Kind.TABLE) {
                hasData = true;
            }
        }

        Version version = Version.empty(options.levelCount());
        Path current = dir.resolve(FileNames.CURRENT);
        if (fs.exists(current)) {
            version = replayManifest(CurrentFile.read(fs, dir), version);
        } else if (hasData) {
            // Without CURRENT the set of live files is unknown. Never guess.
            throw new CorruptionException(current.toString(), 0,
                    "CURRENT is missing, but the directory holds log or table files");
        }

        for (FileMeta file : version.allFiles()) {
            openTable(file);
        }
        nextFileNumber.set(Math.max(version.nextFileNumber(), maxNumber + 1));

        // Replay the logs that the table files do not cover yet.
        MemTable recovered = new MemTable();
        long[] sequence = {version.lastSequence()};
        var live = logs.tailMap(version.minWalNumber(), true);
        for (var entry : live.entrySet()) {
            boolean sealed = !entry.getKey().equals(live.lastKey());
            replayLog(entry.getValue(), entry.getKey(), sealed, sequence, recovered);
        }
        lastSequence = sequence[0];

        // Everything recovered from logs goes into a table file, so that the new MemTable
        // starts with a log of its own.
        if (!recovered.isEmpty()) {
            version = version.apply(new VersionEdit().addFile(0, writeTable(recovered)));
        }
        walNumber = nextFileNumber.getAndIncrement();
        manifestNumber = nextFileNumber.getAndIncrement();
        version = version.apply(new VersionEdit()
                .nextFileNumber(nextFileNumber.get())
                .lastSequence(sequence[0])
                .minWalNumber(walNumber));

        // A new manifest that describes the complete state replaces the old one.
        manifest = new LogWriter(fs.create(FileNames.manifest(dir, manifestNumber)),
                manifestNumber, FileKind.MANIFEST);
        manifest.addRecord(version.toEdit().encode());
        manifest.sync();
        fs.syncDirectory(dir);
        CurrentFile.write(fs, dir, manifestNumber);

        // The old manifest and all old logs are obsolete from here on. The new log is
        // created only now: until CURRENT names the new manifest, a crash must leave the
        // directory as the old manifest describes it.
        wal = new LogWriter(fs.create(FileNames.wal(dir, walNumber)), walNumber, FileKind.WAL);
        fs.syncDirectory(dir);
        removeObsoleteFiles();

        state = new ReadState(new MemTable(), walNumber, List.of(), version, tables);
    }

    /** Rebuilds the version from the manifest that CURRENT names. */
    private Version replayManifest(long number, Version start) throws IOException {
        Path path = FileNames.manifest(dir, number);
        String name = path.toString();
        Version[] version = {start};
        int[] edits = {0};
        ReadableFile file;
        try {
            file = fs.open(path);
        } catch (NoSuchFileException e) {
            throw new CorruptionException(name, 0, "the manifest named by CURRENT is missing", e);
        }
        try (file) {
            // The manifest in use is always the last of its kind. A torn tail holds only
            // edits that never took effect; it disappears when the manifest is replaced.
            LogReader.read(file, name, number, FileKind.MANIFEST, false, (offset, payload) -> {
                try {
                    version[0] = version[0].apply(VersionEdit.decode(payload));
                    edits[0]++;
                } catch (VersionEdit.UnknownTagException e) {
                    throw new UnsupportedFormatException("manifest " + name + ": " + e.getMessage());
                } catch (DecodeException e) {
                    throw new CorruptionException(name, offset, "malformed edit: " + e.getMessage(), e);
                }
            });
        }
        if (edits[0] == 0) {
            // CURRENT is switched only after the first edit of a manifest is durable.
            throw new CorruptionException(name, 0, "the manifest named by CURRENT holds no edit");
        }
        return version[0];
    }

    /** Opens the reader of a table file that the manifest lists. */
    private void openTable(FileMeta file) throws IOException {
        Path path = FileNames.table(dir, file.number());
        try {
            long size = fs.size(path);
            if (size != file.size()) {
                throw new CorruptionException(path.toString(), 0, "table has " + size
                        + " bytes, the manifest records " + file.size());
            }
            tables.put(file.number(), SSTableReader.open(fs, path, file.number(), cache, metadataBudget));
        } catch (NoSuchFileException e) {
            throw new CorruptionException(path.toString(), 0,
                    "a table listed in the manifest is missing", e);
        }
    }

    /** Replays one log into {@code target} and seals the file. */
    private void replayLog(Path path, long number, boolean sealed, long[] sequence,
                           MemTable target) throws IOException {
        String name = path.toString();
        LogReader.Result result;
        try (ReadableFile file = fs.open(path)) {
            result = LogReader.read(file, name, number, FileKind.WAL, sealed, (offset, payload) -> {
                try {
                    long start = WriteBatch.startSequence(payload);
                    long count = WriteBatch.entryCount(payload);
                    // The logs continue the write history without a gap.
                    if (start != sequence[0] + 1 || count == 0) {
                        throw new DecodeException("batch starts at sequence " + start
                                + " with " + count + " entries, expected " + (sequence[0] + 1));
                    }
                    WriteBatch.decode(payload, inserter(target));
                    sequence[0] = start + count - 1;
                } catch (DecodeException e) {
                    throw new CorruptionException(name, offset, "malformed batch: " + e.getMessage(), e);
                }
            });
        }
        if (!sealed) {
            // Cuts a torn tail and makes the remaining contents durable.
            fs.truncate(path, result.validLength());
        }
    }

    private static WriteBatch.Handler inserter(MemTable target) {
        return new WriteBatch.Handler() {
            @Override
            public void put(long sequence, byte[] key, byte[] value) {
                target.put(sequence, key, value);
            }

            @Override
            public void delete(long sequence, byte[] key) {
                target.delete(sequence, key);
            }
        };
    }

    /**
     * Deletes every numbered file the engine no longer needs, and temporary files. A file
     * that cannot be deleted now is left for the next open.
     */
    private void removeObsoleteFiles() throws IOException {
        for (Path path : fs.list(dir)) {
            FileNames.Numbered file = FileNames.parse(path);
            boolean obsolete;
            if (file == null) {
                obsolete = path.getFileName().toString().endsWith(".tmp");
            } else {
                obsolete = switch (file.kind()) {
                    case WAL -> file.number() != walNumber;
                    case TABLE -> !tables.containsKey(file.number());
                    case MANIFEST -> file.number() != manifestNumber;
                };
            }
            if (obsolete) {
                deleteQuietly(path);
            }
        }
    }

    /** Deletes a file; a file that cannot be deleted now is left for the next open. */
    private void deleteQuietly(Path path) {
        try {
            fs.delete(path);
        } catch (IOException e) {
            // Left for the next open.
        }
    }

    // ---- writes ----------------------------------------------------------------------------

    /** Stores {@code value} under {@code key} durably. */
    public void put(byte[] key, byte[] value) throws IOException {
        write(new WriteBatch().put(key, value), true);
    }

    /** Deletes {@code key} durably. */
    public void delete(byte[] key) throws IOException {
        write(new WriteBatch().delete(key), true);
    }

    /**
     * Applies {@code batch} atomically.
     *
     * @param sync whether to wait until the write is durable. A write with {@code false} can
     *             be lost in a crash, together with every later unsynced write.
     * @throws IllegalArgumentException if a key, a value, or the batch exceeds its limit
     * @throws WriteStalledException    if the write waited too long for flush or compaction
     * @throws EngineFailedException    if the engine has failed
     */
    public void write(WriteBatch batch, boolean sync) throws IOException {
        if (batch.largestKeySize() > options.maxKeySize()) {
            throw new IllegalArgumentException("key of " + batch.largestKeySize()
                    + " bytes exceeds the limit of " + options.maxKeySize());
        }
        if (batch.largestValueSize() > options.maxValueSize()) {
            throw new IllegalArgumentException("value of " + batch.largestValueSize()
                    + " bytes exceeds the limit of " + options.maxValueSize());
        }
        if (batch.encodedSize() > options.maxBatchSize()) {
            throw new IllegalArgumentException("batch of " + batch.encodedSize()
                    + " bytes exceeds the limit of " + options.maxBatchSize());
        }
        if (batch.count() == 0) {
            return;
        }

        lock.lock();
        try {
            Pending self = new Pending(batch, sync, lock.newCondition());
            awaitTurn(self);
            if (!self.done) {
                commitGroup(); // this writer is at the head and commits for the group
            }
            if (self.error != null) {
                throw self.error;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Writes everything that is in memory to table files now. Returns when it is done.
     */
    public void flush() throws IOException {
        lock.lock();
        try {
            awaitTurn(new Pending(null, false, lock.newCondition()));
            IOException error = rejection();
            if (error == null) {
                error = runUnlocked(() -> {
                    while (flushOldest()) {
                        // Older MemTables go first.
                    }
                    freezeMemTable();
                    while (flushOldest()) {
                        // Now the one that was just frozen.
                    }
                });
            }
            finishTurn();
            if (error != null) {
                throw error;
            }
        } finally {
            lock.unlock();
        }
        wakeBackground(); // level 0 has grown
    }

    /** Queues {@code self} and waits until it is at the head or a group leader completed it. */
    private void awaitTurn(Pending self) {
        queue.addLast(self);
        while (!self.done && queue.peekFirst() != self) {
            self.turn.awaitUninterruptibly();
        }
    }

    /** Removes the head of the queue and wakes the next request. */
    private void finishTurn() {
        queue.pollFirst();
        Pending next = queue.peekFirst();
        if (next != null) {
            next.turn.signal();
        }
    }

    /** Returns why the engine accepts no changes, or {@code null} if it does. */
    private IOException rejection() {
        if (closed) {
            return new IOException("engine is closed");
        }
        IOException cause = failure;
        if (cause != null) {
            return new EngineFailedException("engine has failed and rejects writes", cause);
        }
        return null;
    }

    /**
     * Runs {@code action} without {@code lock}, on behalf of the request at the head of the
     * queue. A failure puts the engine into the failed state, except for a stall that ran
     * out of time, which fails only the request.
     *
     * @return the failure, or {@code null}
     */
    private IOException runUnlocked(Action action) {
        IOException error = null;
        boolean fatal = true;
        lock.unlock();
        try {
            action.run();
        } catch (WriteStalledException e) {
            error = e;
            fatal = false;
        } catch (EngineFailedException e) {
            error = e; // the engine has failed already
        } catch (IOException e) {
            error = new EngineFailedException("a write to storage failed", e);
        } catch (RuntimeException e) {
            error = new EngineFailedException("unexpected failure while changing the engine", e);
        } finally {
            lock.lock();
        }
        if (error != null && fatal && failure == null) {
            failure = error;
            progress.signalAll();
        }
        return error;
    }

    /** Puts the engine into the failed state, from a thread that does not hold {@code lock}. */
    private void fail(IOException cause) {
        lock.lock();
        try {
            if (failure == null) {
                failure = cause;
            }
            progress.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Commits the writers at the head of the queue as one group. Called with the lock held
     * by the writer at the head; the lock is released during I/O.
     */
    private void commitGroup() {
        List<Pending> group = new ArrayList<>();
        IOException error = rejection();
        if (error != null) {
            group.add(queue.peekFirst());
        } else {
            // A group syncs if its first writer wants a sync. Writers that want a sync do
            // not join a group that will not sync.
            boolean sync = queue.peekFirst().sync;
            long bytes = 0;
            for (Pending write : queue) {
                if (write.batch == null) {
                    break; // a flush or close takes its own turn
                }
                if (!group.isEmpty()
                        && ((write.sync && !sync) || bytes + write.batch.encodedSize() > options.maxGroupCommitSize())) {
                    break;
                }
                group.add(write);
                bytes += write.batch.encodedSize();
            }

            error = runUnlocked(() -> {
                makeRoom();
                long first = lastSequence + 1;
                long next = first;
                for (Pending write : group) {
                    wal.addRecord(write.batch.encode(next));
                    next += write.batch.count();
                }
                if (sync) {
                    wal.sync();
                }
                // Logged; now make the entries readable.
                WriteBatch.Handler inserter = inserter(state.mem());
                next = first;
                for (Pending write : group) {
                    write.batch.forEach(next, inserter);
                    next += write.batch.count();
                }
                lastSequence = next - 1;
            });
        }

        for (Pending write : group) {
            Pending head = queue.pollFirst();
            assert head == write;
            write.error = error;
            write.done = true;
            write.turn.signal();
        }
        Pending next = queue.peekFirst();
        if (next != null) {
            next.turn.signal();
        }
    }

    // ---- backpressure ----------------------------------------------------------------------

    /**
     * Makes sure the MemTable has room for the next group, and holds writes back while flush
     * or compaction is behind (docs/spec/storage-engine.md, section 7). Runs on behalf of
     * the writer at the head of the queue, before it writes anything.
     */
    private void makeRoom() throws IOException {
        long deadline = 0;
        boolean delayed = false;
        while (true) {
            IOException failed = failure;
            if (failed != null) {
                throw new EngineFailedException("engine has failed and rejects writes", failed);
            }
            ReadState current = state;
            int level0 = current.version().files(0).size();

            if (level0 >= options.level0StallTrigger()) {
                if (!options.deterministic()) {
                    deadline = awaitProgress(deadline, "level 0 holds " + level0 + " files");
                    continue;
                }
                boolean compacted;
                try {
                    compacted = compactOnce();
                } catch (IOException e) {
                    compacted = false; // reported by the next explicit compaction
                }
                if (compacted) {
                    continue;
                }
                // Nothing can be compacted now. Without a background thread, waiting would
                // never end, so the write goes on.
            } else if (level0 >= options.level0SlowdownTrigger() && !options.deterministic() && !delayed) {
                // Give compaction a little time, once per group.
                delayed = true;
                wakeBackground();
                heldBackWriters.incrementAndGet();
                try {
                    LockSupport.parkNanos(1_000_000L);
                } finally {
                    heldBackWriters.decrementAndGet();
                }
                continue;
            }

            if (current.mem().approximateBytes() < options.memTableSize()) {
                return;
            }
            if (current.frozen().size() >= options.maxImmutableMemTables()) {
                if (options.deterministic()) {
                    flushOldest();
                } else {
                    deadline = awaitProgress(deadline, current.frozen().size() + " MemTables wait for their flush");
                }
                continue;
            }
            freezeMemTable();
            if (options.deterministic()) {
                flushOldest();
            } else {
                wakeBackground();
            }
        }
    }

    /**
     * Waits until a flush or compaction finishes, the engine fails or is being closed, or
     * the stall time is over.
     *
     * @param deadline the deadline of this write, or 0 if it has not waited yet
     * @return the deadline
     */
    private long awaitProgress(long deadline, String reason) throws IOException {
        long now = options.clock().nanoTime();
        if (deadline == 0) {
            deadline = now + options.writeStallTimeoutMillis() * 1_000_000L;
            if (deadline == 0) {
                deadline = 1;
            }
        }
        heldBackWriters.incrementAndGet();
        lock.lock();
        try {
            if (closing) {
                throw new WriteStalledException("the engine is being closed while the write waits: " + reason);
            }
            long remaining = deadline - now;
            if (remaining <= 0) {
                throw new WriteStalledException("the write stalled for "
                        + options.writeStallTimeoutMillis() + " ms: " + reason);
            }
            backgroundWake = true;
            backgroundSignal.signal();
            try {
                progress.awaitNanos(remaining);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new WriteStalledException("interrupted while the write waited: " + reason);
            }
        } finally {
            lock.unlock();
            heldBackWriters.decrementAndGet();
        }
        return deadline;
    }

    /** Tells waiting writers that flush or compaction made progress. */
    private void signalProgress() {
        lock.lock();
        try {
            progress.signalAll();
        } finally {
            lock.unlock();
        }
    }

    // ---- flush -----------------------------------------------------------------------------

    /**
     * Seals the log of the MemTable, starts a new log and a new MemTable, and puts the old
     * MemTable into the list of those that wait for their flush. Does nothing if the
     * MemTable is empty. Runs on behalf of the request at the head of the queue.
     */
    private void freezeMemTable() throws IOException {
        if (state.mem().isEmpty()) {
            return;
        }
        // A log is synced and closed before its successor is created.
        wal.sync();
        wal.close();
        long sealedWal = walNumber;
        walNumber = nextFileNumber.getAndIncrement();
        wal = new LogWriter(fs.create(FileNames.wal(dir, walNumber)), walNumber, FileKind.WAL);
        fs.syncDirectory(dir);

        stateLock.lock();
        try {
            ReadState before = state;
            List<Frozen> frozen = new ArrayList<>(before.frozen());
            frozen.add(new Frozen(before.mem(), sealedWal, lastSequence));
            publish(new ReadState(new MemTable(), walNumber, frozen, before.version(), tables));
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Writes the oldest waiting MemTable to a table file in level 0, records the file in the
     * manifest, and deletes the MemTable's log.
     *
     * @return whether a MemTable was flushed
     */
    private boolean flushOldest() throws IOException {
        flushLock.lock();
        try {
            ReadState before = state;
            if (before.frozen().isEmpty()) {
                return false;
            }
            Frozen oldest = before.frozen().get(0);

            // 1. The table file, durable before the manifest mentions it.
            FileMeta file = writeTable(oldest.mem());

            // 2. One manifest edit makes the file live and the MemTable's log obsolete. It
            //    takes effect only once it is synced.
            stateLock.lock();
            try {
                ReadState now = state;
                List<Frozen> remaining = now.frozen().subList(1, now.frozen().size());
                long oldestLiveWal = remaining.isEmpty() ? now.memWalNumber() : remaining.get(0).walNumber();
                VersionEdit edit = new VersionEdit()
                        .addFile(0, file)
                        .nextFileNumber(nextFileNumber.get())
                        .lastSequence(oldest.lastSequence())
                        .minWalNumber(oldestLiveWal);
                manifest.addRecord(edit.encode());
                manifest.sync();
                publish(new ReadState(now.mem(), now.memWalNumber(), remaining, now.version().apply(edit), tables));
                rollManifestIfLarge();
            } finally {
                stateLock.unlock();
            }

            // 3. The log is no longer needed.
            deleteQuietly(FileNames.wal(dir, oldest.walNumber()));
            signalProgress();
            return true;
        } finally {
            flushLock.unlock();
        }
    }

    /**
     * Replaces the manifest by a new one that holds only the current state, once it has
     * grown past its limit. Called with {@code stateLock} held, after an edit took effect.
     * A failure leaves it unknown which manifest {@code CURRENT} names, so it stops the
     * engine.
     */
    private void rollManifestIfLarge() throws IOException {
        if (manifest.size() < options.maxManifestSize()) {
            return;
        }
        try {
            long number = nextFileNumber.getAndIncrement();
            Version snapshot = state.version().apply(new VersionEdit().nextFileNumber(nextFileNumber.get()));
            LogWriter next = new LogWriter(fs.create(FileNames.manifest(dir, number)), number, FileKind.MANIFEST);
            try {
                next.addRecord(snapshot.toEdit().encode());
                next.sync();
                fs.syncDirectory(dir);
                CurrentFile.write(fs, dir, number);
            } catch (IOException | RuntimeException e) {
                next.close();
                throw e;
            }
            LogWriter previous = manifest;
            long previousNumber = manifestNumber;
            manifest = next;
            manifestNumber = number;
            previous.close();
            deleteQuietly(FileNames.manifest(dir, previousNumber));
        } catch (IOException e) {
            EngineFailedException failed = new EngineFailedException("replacing the manifest failed", e);
            fail(failed);
            throw failed;
        }
    }

    /** Writes all entries of {@code source} to a new table file and opens its reader. */
    private FileMeta writeTable(MemTable source) throws IOException {
        long number = nextFileNumber.getAndIncrement();
        Path path = FileNames.table(dir, number);
        SSTableWriter.TableInfo info;
        try (SSTableWriter writer = new SSTableWriter(fs, path, number)) {
            for (Map.Entry<byte[], byte[]> entry : source.entries()) {
                writer.add(entry.getKey(), entry.getValue());
            }
            info = writer.finish();
        }
        fs.syncDirectory(dir);
        tables.put(number, SSTableReader.open(fs, path, number, cache, metadataBudget));
        return new FileMeta(number, info.fileSize(), info.smallestKey(), info.largestKey(),
                info.smallestSequence(), info.largestSequence());
    }

    // ---- background work -------------------------------------------------------------------

    /**
     * Does one unit of pending background work: the flush of a waiting MemTable, or else one
     * compaction. In deterministic mode the caller drives the engine with this method; in
     * normal mode the background thread does.
     *
     * @return whether there was something to do
     * @throws CorruptionException if a compaction input is damaged
     */
    public boolean runBackgroundWork() throws IOException {
        boolean flushed;
        try {
            flushed = flushOldest();
        } catch (IOException e) {
            // The manifest or the table may be half written: no further writes.
            EngineFailedException failed = new EngineFailedException("a flush failed", e);
            fail(failed);
            throw failed;
        }
        return flushed || compactOnce();
    }

    private void wakeBackground() {
        if (backgroundThread == null) {
            return;
        }
        lock.lock();
        try {
            backgroundWake = true;
            backgroundSignal.signal();
        } finally {
            lock.unlock();
        }
    }

    private void backgroundLoop() {
        while (true) {
            lock.lock();
            try {
                while (!backgroundWake && !closed) {
                    backgroundSignal.awaitUninterruptibly();
                }
                if (closed) {
                    return;
                }
                backgroundWake = false;
            } finally {
                lock.unlock();
            }
            try {
                while (!closed && failure == null && runBackgroundWork()) {
                    lastCompactionProblem = null;
                }
            } catch (IOException | RuntimeException e) {
                // A failed flush has put the engine into the failed state. A failed
                // compaction is kept for the caller of compact(); the next wake retries.
                lastCompactionProblem = (e instanceof IOException io) ? io : new IOException(e);
            }
        }
    }

    /**
     * Returns the problem that ended the last background compaction, or {@code null} if it
     * succeeded or none ran.
     */
    public IOException lastCompactionProblem() {
        return lastCompactionProblem;
    }

    // ---- checkpoints -----------------------------------------------------------------------

    /**
     * Creates a consistent copy of the engine in {@code target}, a directory on the same file
     * system that does not exist yet (docs/spec/storage-engine.md, section 9).
     *
     * <p>The copy holds everything written before the call. Table files are hard links, so
     * the copy takes almost no time and no space at first. It is a complete engine directory
     * and can be opened on its own. This engine keeps running unchanged.
     *
     * <p>If the call fails, {@code target} may hold an incomplete copy without a
     * {@code CURRENT} file; such a directory cannot be opened and should be removed.
     */
    public void checkpoint(Path target) throws IOException {
        if (fs.exists(target)) {
            throw new IOException("the checkpoint target exists already: " + target);
        }
        // After the flush the table files hold everything written before this call. The
        // pinned version may hold more; it is a consistent state in any case.
        flush();
        ReadState pinned = pinState();
        try {
            Version version = pinned.version();
            fs.createDirectories(target);
            // The pin keeps every table file in place while it is linked.
            for (FileMeta file : version.allFiles()) {
                fs.link(FileNames.table(dir, file.number()), FileNames.table(target, file.number()));
            }
            long number = nextFileNumber.getAndIncrement();
            Version copy = version.apply(new VersionEdit()
                    .nextFileNumber(number + 1)
                    .minWalNumber(number + 1)); // the copy has no log
            try (LogWriter writer = new LogWriter(
                    fs.create(FileNames.manifest(target, number)), number, FileKind.MANIFEST)) {
                writer.addRecord(copy.toEdit().encode());
                writer.sync();
            }
            fs.syncDirectory(target);
            CurrentFile.write(fs, target, number);
            Path parent = target.toAbsolutePath().getParent();
            if (parent != null) {
                fs.syncDirectory(parent);
            }
        } finally {
            pinned.unpin();
        }
    }

    // ---- compaction ------------------------------------------------------------------------

    /**
     * Runs compactions until no level needs one.
     *
     * @return whether at least one compaction ran
     * @throws CorruptionException if an input table is damaged; the compaction is abandoned
     *                             and nothing of the damaged table is copied
     */
    public boolean compact() throws IOException {
        boolean any = false;
        while (compactOnce()) {
            any = true;
        }
        return any;
    }

    /** Runs the most urgent compaction, if any level needs one. */
    private boolean compactOnce() throws IOException {
        compactionLock.lock();
        try {
            ReadState pinned = pinState();
            try {
                CompactionPlan plan = pickCompaction(pinned.version());
                if (plan == null) {
                    return false;
                }
                runCompaction(pinned, plan);
                // Only success counts as progress: writers that wait for a compaction that
                // keeps failing wait until their time is over instead of retrying in a loop.
                signalProgress();
                return true;
            } finally {
                pinned.unpin();
            }
        } finally {
            compactionLock.unlock();
        }
    }

    /** Chooses the level that exceeds its limit the most, or returns {@code null}. */
    private CompactionPlan pickCompaction(Version version) {
        int best = -1;
        double bestScore = 1.0;
        double score = (double) version.files(0).size() / options.level0CompactionTrigger();
        if (score >= bestScore) {
            best = 0;
            bestScore = score;
        }
        // The last level has nowhere to go.
        for (int level = 1; level < version.levelCount() - 1; level++) {
            score = (double) version.levelBytes(level) / options.levelTargetSize(level);
            if (score >= 1.0 && score > bestScore) {
                best = level;
                bestScore = score;
            }
        }
        if (best < 0) {
            return null;
        }

        List<FileMeta> inputs;
        if (best == 0) {
            // Level-0 files overlap each other, so all of them go together.
            inputs = version.files(0);
        } else {
            // The oldest file of the level.
            FileMeta oldest = version.files(best).get(0);
            for (FileMeta file : version.files(best)) {
                if (file.number() < oldest.number()) {
                    oldest = file;
                }
            }
            inputs = List.of(oldest);
        }
        byte[] smallest = null;
        byte[] largest = null;
        for (FileMeta file : inputs) {
            byte[] low = InternalKey.userKey(file.smallestKey());
            byte[] high = InternalKey.userKey(file.largestKey());
            if (smallest == null || ByteComparator.compare(low, smallest) < 0) {
                smallest = low;
            }
            if (largest == null || ByteComparator.compare(high, largest) > 0) {
                largest = high;
            }
        }
        return new CompactionPlan(best, inputs, version.overlapping(best + 1, smallest, largest));
    }

    /**
     * Merges the inputs of {@code plan} into new files of the next level and makes the
     * result live with one manifest edit.
     */
    private void runCompaction(ReadState pinned, CompactionPlan plan) throws IOException {
        long[] liveSnapshots;
        synchronized (snapshots) {
            liveSnapshots = snapshots.keySet().stream().mapToLong(Long::longValue).toArray();
        }
        List<FileMeta> allInputs = new ArrayList<>(plan.inputs());
        allInputs.addAll(plan.nextLevelInputs());

        // 1. Merge. Every block read is verified; a damaged input aborts the compaction.
        List<FileMeta> outputs = new ArrayList<>();
        Map<Long, SSTableReader> outputReaders = new HashMap<>();
        boolean installed = false;
        boolean editAttempted = false;
        try {
            writeCompactionOutputs(pinned, plan, allInputs, new Retention(liveSnapshots), outputs);
            fs.syncDirectory(dir);
            for (FileMeta output : outputs) {
                outputReaders.put(output.number(), SSTableReader.open(
                        fs, FileNames.table(dir, output.number()), output.number(), cache, metadataBudget));
            }

            // 2. One edit replaces the inputs by the outputs.
            VersionEdit edit = new VersionEdit();
            for (FileMeta input : plan.inputs()) {
                edit.removeFile(plan.level(), input.number());
            }
            for (FileMeta input : plan.nextLevelInputs()) {
                edit.removeFile(plan.outputLevel(), input.number());
            }
            for (FileMeta output : outputs) {
                edit.addFile(plan.outputLevel(), output);
            }
            stateLock.lock();
            try {
                IOException rejected = rejection();
                if (rejected != null) {
                    throw rejected;
                }
                edit.nextFileNumber(nextFileNumber.get());
                // From here on the edit may be durable, so the outputs must stay.
                editAttempted = true;
                try {
                    manifest.addRecord(edit.encode());
                    manifest.sync();
                } catch (IOException e) {
                    EngineFailedException failed = new EngineFailedException("a manifest write failed", e);
                    fail(failed);
                    throw failed;
                }
                tables.putAll(outputReaders);
                ReadState before = state;
                publish(new ReadState(before.mem(), before.memWalNumber(), before.frozen(),
                        before.version().apply(edit), tables));
                rollManifestIfLarge();
            } finally {
                stateLock.unlock();
            }
            installed = true;
        } finally {
            if (!installed && !editAttempted) {
                // Nothing refers to the outputs; remove them.
                for (SSTableReader reader : outputReaders.values()) {
                    try {
                        reader.release();
                    } catch (IOException e) {
                        // The file is removed below or at the next open.
                    }
                }
                for (FileMeta output : outputs) {
                    deleteQuietly(FileNames.table(dir, output.number()));
                }
            }
            // If the edit was attempted and failed, the engine is in the failed state. The
            // next open decides from the manifest which files are live.
        }

        // 3. The inputs have left the current version. Each is deleted as soon as the last
        //    reader that still uses it lets go.
        for (FileMeta input : allInputs) {
            SSTableReader reader = tables.remove(input.number());
            Path path = FileNames.table(dir, input.number());
            reader.whenUnused(() -> {
                try {
                    cache.evictFile(input.number());
                } catch (RuntimeException e) {
                    // The cache is already closed; nothing to evict.
                }
                deleteQuietly(path);
            });
            reader.release();
        }
    }

    /** Merges all inputs, applies the retention rules, and writes the output files. */
    private void writeCompactionOutputs(ReadState pinned, CompactionPlan plan, List<FileMeta> allInputs,
                                        Retention retention, List<FileMeta> outputs) throws IOException {
        List<InternalIterator> sources = new ArrayList<>();
        try {
            for (FileMeta input : allInputs) {
                sources.add(pinned.reader(input.number()).iterator());
            }
        } catch (IOException | RuntimeException e) {
            for (InternalIterator source : sources) {
                source.close();
            }
            throw e;
        }
        Version version = pinned.version();
        SSTableWriter writer = null;
        long writerNumber = 0;
        byte[] userKey = null;
        boolean deeperLevelsMayHold = false;
        try (MergingIterator merged = new MergingIterator(sources)) {
            for (merged.seekToFirst(); merged.valid(); merged.next()) {
                byte[] key = merged.key();
                boolean newUserKey = userKey == null || !InternalKey.hasUserKey(key, userKey);
                if (newUserKey) {
                    userKey = InternalKey.userKey(key);
                    deeperLevelsMayHold = false;
                    for (int level = plan.outputLevel() + 1; level < version.levelCount(); level++) {
                        if (version.fileContaining(level, userKey) != null) {
                            deeperLevelsMayHold = true;
                            break;
                        }
                    }
                }
                if (!retention.keep(key, deeperLevelsMayHold)) {
                    continue;
                }
                // A file is split only between two user keys, so that no user key occurs in
                // two files of the output level.
                if (writer != null && newUserKey && writer.fileSize() >= options.targetFileSize()) {
                    outputs.add(finishOutput(writer, writerNumber));
                    writer = null;
                }
                if (writer == null) {
                    writerNumber = nextFileNumber.getAndIncrement();
                    writer = new SSTableWriter(fs, FileNames.table(dir, writerNumber), writerNumber);
                }
                byte[] value = merged.value();
                compactionLimiter.acquire(key.length + value.length);
                writer.add(key, value);
            }
            if (writer != null) {
                outputs.add(finishOutput(writer, writerNumber));
                writer = null;
            }
        } finally {
            if (writer != null) {
                // Abandoned in the middle of a file.
                writer.close();
                deleteQuietly(FileNames.table(dir, writerNumber));
            }
        }
    }

    private static FileMeta finishOutput(SSTableWriter writer, long number) throws IOException {
        SSTableWriter.TableInfo info = writer.finish();
        return new FileMeta(number, info.fileSize(), info.smallestKey(), info.largestKey(),
                info.smallestSequence(), info.largestSequence());
    }

    // ---- read state ------------------------------------------------------------------------

    /**
     * Makes {@code next} the state that reads use and drops the engine's pin on the state
     * it replaces. Called with {@code stateLock} held.
     */
    private void publish(ReadState next) throws IOException {
        ReadState previous = state;
        state = next;
        if (previous != null) {
            previous.unpin();
        }
    }

    /** Pins and returns the current read state. The caller must unpin it. */
    private ReadState pinState() {
        while (true) {
            if (shutDown) {
                throw new IllegalStateException("engine is closed");
            }
            ReadState current = state;
            if (current.tryPin()) {
                return current;
            }
            // The state was replaced and released in between; take the new one.
        }
    }

    // ---- reads -----------------------------------------------------------------------------

    /**
     * Returns the value stored under {@code key}, or {@code null} if there is none.
     *
     * @throws CorruptionException if a table block needed for the answer is damaged
     */
    public byte[] get(byte[] key) throws IOException {
        // The sequence number is read before the state: the state read afterward holds at
        // least everything up to it.
        return lookup(key, lastSequence);
    }

    /**
     * Returns the value {@code key} had when {@code snapshot} was taken, or {@code null} if
     * it had none.
     */
    public byte[] get(byte[] key, Snapshot snapshot) throws IOException {
        return lookup(key, snapshot.sequenceFor(this));
    }

    private byte[] lookup(byte[] key, long snapshot) throws IOException {
        if (key.length == 0) {
            throw new IllegalArgumentException("keys must not be empty");
        }
        ReadState current = pinState();
        try {
            Lookup lookup = current.mem().get(key, snapshot);
            // The waiting MemTables, newest first.
            for (int i = current.frozen().size() - 1; i >= 0 && lookup.kind() == Lookup.Kind.ABSENT; i--) {
                lookup = current.frozen().get(i).mem().get(key, snapshot);
            }
            if (lookup.kind() == Lookup.Kind.ABSENT) {
                // Level 0, newest file first: the first file that knows the key decides.
                for (FileMeta file : current.version().files(0)) {
                    if (!file.mayContainUserKey(key)) {
                        continue;
                    }
                    lookup = current.reader(file.number()).get(key, snapshot);
                    if (lookup.kind() != Lookup.Kind.ABSENT) {
                        break;
                    }
                }
            }
            // Deeper levels, from newer to older data: at most one file per level.
            for (int level = 1; lookup.kind() == Lookup.Kind.ABSENT && level < current.version().levelCount(); level++) {
                FileMeta file = current.version().fileContaining(level, key);
                if (file != null) {
                    lookup = current.reader(file.number()).get(key, snapshot);
                }
            }
            return lookup.kind() == Lookup.Kind.VALUE ? lookup.value() : null;
        } finally {
            current.unpin();
        }
    }

    // ---- snapshots and scans ---------------------------------------------------------------

    /**
     * Takes a snapshot of the current contents. It must be closed when no longer needed.
     */
    public Snapshot snapshot() {
        synchronized (snapshots) {
            long sequence = lastSequence;
            snapshots.merge(sequence, 1, Integer::sum);
            return new Snapshot(this, sequence);
        }
    }

    void releaseSnapshot(Snapshot snapshot) {
        synchronized (snapshots) {
            snapshots.computeIfPresent(snapshot.sequence(), (k, count) -> count > 1 ? count - 1 : null);
        }
    }

    /** Returns the number of snapshots that are not closed. */
    public int liveSnapshotCount() {
        synchronized (snapshots) {
            int total = 0;
            for (int count : snapshots.values()) {
                total += count;
            }
            return total;
        }
    }

    /**
     * Returns the sequence number of the oldest live snapshot, or the current last sequence
     * number if there is none. Versions that are only visible below it are not needed.
     */
    public long oldestSnapshotSequence() {
        synchronized (snapshots) {
            return snapshots.isEmpty() ? lastSequence : snapshots.firstKey();
        }
    }

    /**
     * Scans the keys from {@code start} (inclusive) to {@code end} (exclusive) in their
     * current state.
     *
     * @param start the first key, or {@code null} to start at the smallest key
     * @param end   the key to stop before, or {@code null} to scan to the largest key
     */
    public ScanIterator scan(byte[] start, byte[] end) throws IOException {
        return scan(start, end, lastSequence);
    }

    /** Scans a range as it was when {@code snapshot} was taken. */
    public ScanIterator scan(byte[] start, byte[] end, Snapshot snapshot) throws IOException {
        return scan(start, end, snapshot.sequenceFor(this));
    }

    private ScanIterator scan(byte[] start, byte[] end, long snapshot) throws IOException {
        if ((start != null && start.length == 0) || (end != null && end.length == 0)) {
            throw new IllegalArgumentException("range bounds must not be empty; use null for no bound");
        }
        ReadState current = pinState();
        List<InternalIterator> sources = new ArrayList<>();
        try {
            sources.add(current.mem().iterator());
            for (Frozen frozen : current.frozen()) {
                sources.add(frozen.mem().iterator());
            }
            for (FileMeta file : current.version().allFiles()) {
                if (overlaps(file, start, end)) {
                    sources.add(current.reader(file.number()).iterator());
                }
            }
        } catch (IOException | RuntimeException e) {
            for (InternalIterator source : sources) {
                source.close();
            }
            current.unpin();
            throw e;
        }
        openScans.incrementAndGet();
        // The scan keeps the state pinned, so its tables stay in place until it is closed.
        return new ScanIterator(new MergingIterator(sources), snapshot, start, end, () -> {
            openScans.decrementAndGet();
            current.unpin();
        });
    }

    /** Returns whether the user-key range of {@code file} intersects {@code [start, end)}. */
    private static boolean overlaps(FileMeta file, byte[] start, byte[] end) {
        byte[] smallest = file.smallestKey();
        byte[] largest = file.largestKey();
        if (end != null && ByteComparator.compare(smallest, 0, InternalKey.userKeyLength(smallest),
                end, 0, end.length) >= 0) {
            return false;
        }
        return start == null || ByteComparator.compare(largest, 0, InternalKey.userKeyLength(largest),
                start, 0, start.length) >= 0;
    }

    // ---- figures ---------------------------------------------------------------------------

    /** Returns the number of scans that are not closed. */
    public int openScanCount() {
        return openScans.get();
    }

    /** Returns the sequence number of the last write that readers can see. */
    public long lastSequence() {
        return lastSequence;
    }

    /** Returns the number of live table files in {@code level}. */
    public int tableCount(int level) {
        return state.version().files(level).size();
    }

    /** Returns the number of full MemTables that wait for their flush. */
    public int waitingMemTableCount() {
        return state.frozen().size();
    }

    /** Returns the budget for the index and filter blocks of this engine's tables. */
    public TableMetadataBudget tableMetadataBudget() {
        return metadataBudget;
    }

    /** Returns the number of the manifest file in use. */
    public long manifestNumber() {
        stateLock.lock();
        try {
            return manifestNumber;
        } finally {
            stateLock.unlock();
        }
    }

    /** Returns the total time compaction was made to wait by the I/O rate limit. */
    public long compactionThrottledNanos() {
        return compactionLimiter.throttledNanos();
    }

    /** Returns the block cache of this engine. */
    public BlockCache blockCache() {
        return cache;
    }

    // ---- lifecycle -------------------------------------------------------------------------

    /**
     * Syncs the log and releases every file and the block cache. Writes that are queued or
     * arrive later fail. MemTables that still wait for their flush stay in their logs and
     * are recovered at the next open. Closing twice has no effect.
     */
    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closing = true;
            progress.signalAll(); // writers that wait for background work give up
            // Closing takes a turn in the queue, so it never runs during a commit.
            awaitTurn(new Pending(null, false, lock.newCondition()));
            finishTurn();
            if (closed) {
                return; // another thread closed the engine while this one waited
            }
            closed = true;
            backgroundSignal.signalAll();
            progress.signalAll();
        } finally {
            lock.unlock();
        }

        // The background thread finishes the unit it is working on.
        if (backgroundThread != null) {
            boolean interrupted = false;
            while (true) {
                try {
                    backgroundThread.join();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        // Wait for a flush or compaction that a caller's thread may be running.
        flushLock.lock();
        compactionLock.lock();
        stateLock.lock();
        try {
            shutDown = true;
            Throwable problem = null;
            if (failure == null) {
                try {
                    wal.sync();
                } catch (IOException e) {
                    problem = e;
                }
            }
            problem = releaseResources(problem);
            if (problem instanceof IOException e) {
                throw e;
            }
            if (problem instanceof RuntimeException e) {
                throw e;
            }
        } finally {
            stateLock.unlock();
            compactionLock.unlock();
            flushLock.unlock();
        }
    }

    /**
     * Closes everything the engine holds. Problems are attached to {@code problem} as
     * suppressed exceptions, or the first one is returned if {@code problem} is {@code null}.
     */
    private Throwable releaseResources(Throwable problem) {
        List<Closeable> resources = new ArrayList<>();
        if (wal != null) {
            resources.add(wal);
        }
        if (manifest != null) {
            resources.add(manifest);
        }
        ReadState last = state;
        if (last != null) {
            resources.add(last::unpin);
        }
        for (SSTableReader reader : tables.values()) {
            resources.add(reader::release);
        }
        tables.clear();
        if (cache != null) {
            resources.add(cache::close);
        }
        if (directoryLock != null) {
            resources.add(directoryLock);
        }
        for (Closeable resource : resources) {
            try {
                resource.close();
            } catch (IOException | RuntimeException e) {
                if (problem == null) {
                    problem = e;
                } else {
                    problem.addSuppressed(e);
                }
            }
        }
        return problem;
    }
}
