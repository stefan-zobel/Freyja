package io.github.stefan_zobel.freyja.lsm;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
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

/**
 * The storage engine: a write-ahead log, a MemTable, and table files in level 0, tied
 * together by a manifest. Compaction into deeper levels follows in a later milestone
 * (docs/spec/roadmap.md).
 *
 * <p><b>Writes.</b> One writer at a time appends to the log and the MemTable. Writers that
 * arrive meanwhile queue up and are committed as a group with a single sync
 * (docs/spec/storage-engine.md, section 5). A write with {@code sync = true} returns only
 * after its log record is durable.
 *
 * <p><b>Flush.</b> When the MemTable reaches its size limit, the writer whose turn it is
 * seals the log, starts a new log and a new MemTable, writes the old MemTable to a table
 * file, and records that file in the manifest. There is one log per MemTable.
 *
 * <p><b>Reads</b> never block. They look at the MemTable, at a MemTable that is being
 * flushed, and at the table files, newest first, and see every write that has returned.
 * A {@link Snapshot} fixes the sequence number a read uses; a {@link ScanIterator} walks a
 * key range at one sequence number.
 *
 * <p><b>Failure.</b> If a write to the log, a table, or the manifest fails, the instance
 * enters a failed state and rejects all further writes with {@link EngineFailedException}.
 * Damage found while opening prevents opening; the engine never opens with data loss.
 *
 * <p>Safe for concurrent use. The engine must not be used while or after it is closed, and
 * every scan must be closed before the engine is.
 */
public final class LsmEngine implements Closeable {

    private enum Task { WRITE, FLUSH, CLOSE }

    /** A queued request for the exclusive right to change the engine. */
    private static final class Pending {
        final Task task;
        final WriteBatch batch;
        final boolean sync;
        final Condition turn;
        boolean done;
        IOException error;

        Pending(Task task, WriteBatch batch, boolean sync, Condition turn) {
            this.task = task;
            this.batch = batch;
            this.sync = sync;
            this.turn = turn;
        }
    }

    /**
     * Everything a read looks at, replaced as a whole.
     *
     * @param mem     the MemTable that receives writes
     * @param imm     the MemTable that is being flushed, or {@code null}
     * @param version the live table files
     */
    private record ReadState(MemTable mem, MemTable imm, Version version) {
    }

    private final StorageFileSystem fs;
    private final Path dir;
    private final EngineOptions options;

    private final ReentrantLock lock = new ReentrantLock();
    private final ArrayDeque<Pending> queue = new ArrayDeque<>();
    private IOException failure;      // guarded by lock
    private boolean closed;           // guarded by lock

    // Used only by the request at the head of the queue, and during open.
    private LogWriter wal;
    private long walNumber;
    private LogWriter manifest;
    private long manifestNumber;
    private long nextFileNumber;

    private final Map<Long, SSTableReader> tables = new ConcurrentHashMap<>();
    private BlockCache cache;
    private Closeable directoryLock;

    private volatile ReadState state;

    /** Live snapshots: sequence number to number of snapshots at it. */
    private final TreeMap<Long, Integer> snapshots = new TreeMap<>();
    private final AtomicInteger openScans = new AtomicInteger();

    /** Sequence number of the last write that is visible to readers. */
    private volatile long lastSequence;

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
        fs.createDirectories(dir);
        engine.directoryLock = fs.lock(dir.resolve(FileNames.LOCK));
        try {
            engine.cache = new BlockCache(engine.options.blockCacheCapacity());
            engine.recover();
            return engine;
        } catch (Throwable t) {
            engine.releaseResources(t);
            throw t;
        }
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
        nextFileNumber = Math.max(version.nextFileNumber(), maxNumber + 1);

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
        walNumber = nextFileNumber++;
        manifestNumber = nextFileNumber++;
        version = version.apply(new VersionEdit()
                .nextFileNumber(nextFileNumber)
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

        state = new ReadState(new MemTable(), null, version);
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
            tables.put(file.number(), SSTableReader.open(fs, path, file.number(), cache));
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
                try {
                    fs.delete(path);
                } catch (IOException e) {
                    // Left for the next open.
                }
            }
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
            Pending self = new Pending(Task.WRITE, batch, sync, lock.newCondition());
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
     * Writes the MemTable to a table file now, whatever its size. Does nothing if the
     * MemTable is empty.
     */
    public void flush() throws IOException {
        lock.lock();
        try {
            awaitTurn(new Pending(Task.FLUSH, null, false, lock.newCondition()));
            IOException error = rejection();
            if (error == null) {
                error = runUnlocked(this::flushMemTable);
            }
            finishTurn();
            if (error != null) {
                throw error;
            }
        } finally {
            lock.unlock();
        }
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
        if (failure != null) {
            return new EngineFailedException("engine has failed and rejects writes", failure);
        }
        return null;
    }

    @FunctionalInterface
    private interface Action {
        void run() throws IOException;
    }

    /**
     * Runs {@code action} without the lock, on behalf of the request at the head of the
     * queue. A failure puts the engine into the failed state.
     *
     * @return the failure, or {@code null}
     */
    private IOException runUnlocked(Action action) {
        IOException error = null;
        lock.unlock();
        try {
            action.run();
        } catch (IOException e) {
            error = new EngineFailedException("a write to storage failed", e);
        } catch (RuntimeException e) {
            error = new EngineFailedException("unexpected failure while changing the engine", e);
        } finally {
            lock.lock();
        }
        if (error != null && failure == null) {
            failure = error;
        }
        return error;
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
                if (write.task != Task.WRITE) {
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
                if (state.mem().approximateBytes() >= options.memTableSize()) {
                    flushMemTable();
                }
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

    // ---- flush -----------------------------------------------------------------------------

    /**
     * Moves the contents of the MemTable into a new table file in level 0. Runs on behalf of
     * the request at the head of the queue, so no write is in progress.
     */
    private void flushMemTable() throws IOException {
        ReadState before = state;
        if (before.mem().isEmpty()) {
            return;
        }
        // 1. Seal the log of this MemTable, then start the log of the next one.
        wal.sync();
        wal.close();
        long sealedWal = walNumber;
        walNumber = nextFileNumber++;
        wal = new LogWriter(fs.create(FileNames.wal(dir, walNumber)), walNumber, FileKind.WAL);
        fs.syncDirectory(dir);

        // 2. Writes go to a new MemTable; reads see both until the table file is live.
        MemTable frozen = before.mem();
        state = new ReadState(new MemTable(), frozen, before.version());

        // 3. Write the table file and make it durable.
        FileMeta file = writeTable(frozen);

        // 4. The manifest edit makes the file live and the sealed log obsolete. It takes
        //    effect only once it is synced.
        VersionEdit edit = new VersionEdit()
                .addFile(0, file)
                .nextFileNumber(nextFileNumber)
                .lastSequence(lastSequence)
                .minWalNumber(walNumber);
        manifest.addRecord(edit.encode());
        manifest.sync();
        state = new ReadState(state.mem(), null, before.version().apply(edit));

        // 5. The sealed log is no longer needed.
        try {
            fs.delete(FileNames.wal(dir, sealedWal));
        } catch (IOException e) {
            // Left for the next open.
        }
    }

    /** Writes all entries of {@code source} to a new table file and opens its reader. */
    private FileMeta writeTable(MemTable source) throws IOException {
        long number = nextFileNumber++;
        Path path = FileNames.table(dir, number);
        SSTableWriter.TableInfo info;
        try (SSTableWriter writer = new SSTableWriter(fs, path, number)) {
            for (Map.Entry<byte[], byte[]> entry : source.entries()) {
                writer.add(entry.getKey(), entry.getValue());
            }
            info = writer.finish();
        }
        fs.syncDirectory(dir);
        tables.put(number, SSTableReader.open(fs, path, number, cache));
        return new FileMeta(number, info.fileSize(), info.smallestKey(), info.largestKey(),
                info.smallestSequence(), info.largestSequence());
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
        ReadState current = state;

        Lookup lookup = current.mem().get(key, snapshot);
        if (lookup.kind() == Lookup.Kind.ABSENT && current.imm() != null) {
            lookup = current.imm().get(key, snapshot);
        }
        if (lookup.kind() == Lookup.Kind.ABSENT) {
            // Level 0, newest file first: the first file that knows the key decides.
            for (FileMeta file : current.version().files(0)) {
                if (!file.mayContainUserKey(key)) {
                    continue;
                }
                lookup = tables.get(file.number()).get(key, snapshot);
                if (lookup.kind() != Lookup.Kind.ABSENT) {
                    break;
                }
            }
        }
        return lookup.kind() == Lookup.Kind.VALUE ? lookup.value() : null;
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
        ReadState current = state;
        List<InternalIterator> sources = new ArrayList<>();
        sources.add(current.mem().iterator());
        if (current.imm() != null) {
            sources.add(current.imm().iterator());
        }
        for (FileMeta file : current.version().allFiles()) {
            if (overlaps(file, start, end)) {
                sources.add(tables.get(file.number()).iterator());
            }
        }
        openScans.incrementAndGet();
        return new ScanIterator(new MergingIterator(sources), snapshot, start, end,
                openScans::decrementAndGet);
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

    /** Returns the block cache of this engine. */
    public BlockCache blockCache() {
        return cache;
    }

    // ---- lifecycle -------------------------------------------------------------------------

    /**
     * Syncs the log and releases every file and the block cache. Writes that are queued or
     * arrive later fail. Closing twice has no effect.
     */
    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            // Closing takes a turn in the queue, so it never runs during a commit or flush.
            awaitTurn(new Pending(Task.CLOSE, null, false, lock.newCondition()));
            finishTurn();
            if (closed) {
                return; // another thread closed the engine while this one waited
            }
            closed = true;
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
            lock.unlock();
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
