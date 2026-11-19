package io.github.sz.freyja.node.raftlog;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.github.sz.freyja.common.Clock;
import io.github.sz.freyja.common.LittleEndian;
import io.github.sz.freyja.lsm.CorruptionException;
import io.github.sz.freyja.lsm.UnsupportedFormatException;
import io.github.sz.freyja.lsm.fs.ReadableFile;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.lsm.fs.WritableFile;
import io.github.sz.freyja.lsm.log.FileKind;
import io.github.sz.freyja.lsm.log.LogReader;
import io.github.sz.freyja.lsm.log.LogWriter;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.EntryType;
import io.github.sz.freyja.raft.LogEntry;
import io.github.sz.freyja.raft.LogView;

/**
 * The Raft log of a node on disk (docs/spec/node.md, section 3).
 *
 * <p>The log is a sequence of segment files in the WAL record format. It is append-only: a
 * suffix truncation is a record, and a prefix truncation deletes whole segments. An index in
 * memory says where every stored entry is; the entries of the segment being written are kept
 * in memory as well, the others are read from their files.
 *
 * <p><b>Opening.</b> A torn tail of the last segment is left where it is: the header of the
 * next segment records how much of its predecessor counts. Damage is never cut away either.
 * The store then holds what it can vouch for, reports the first index it cannot vouch for
 * ({@link #firstDamagedIndex}), and accepts no writes until the damaged files have been set
 * aside ({@link #quarantineDamaged}). The node does that only after its hard state says that
 * it is in log-recovering mode, so that a crash in between finds the damage again.
 *
 * <p>A failed write or sync is fatal for the instance. An instance is confined to one thread,
 * except that the sync of the segment being written may run on another ({@link Sync}).
 */
public final class RaftLogStore implements LogView, Closeable {

    /** The directory inside the log directory that takes damaged segments. */
    public static final String DAMAGED_DIRECTORY = "damaged";

    private static final String SUFFIX = ".rlog";
    /** Logical records. The first byte of a record is its type. */
    public static final int RECORD_HEADER = 1;
    public static final int RECORD_ENTRIES = 2;
    public static final int RECORD_TRUNCATE = 3;
    public static final int SEGMENT_VERSION = 1;
    /** The segment header record follows the header record of the file. */
    private static final long HEADER_RECORD_OFFSET = 14;

    /** A segment file. */
    private static final class Segment {
        final long number;
        final Path path;
        /** How much of the file counts; what lies beyond is a torn tail. */
        long validLength;

        Segment(long number, Path path) {
            this.number = number;
            this.path = path;
        }
    }

    /** What the header record of a segment says. */
    public record SegmentHeader(long firstIndex, long previousNumber, long previousValidLength,
                         ClusterConfig config) {
    }

    private final StorageFileSystem fs;
    private final Path directory;
    private final long segmentSize;

    private final List<Segment> segments = new ArrayList<>();
    private final Map<Long, ReadableFile> readers = new HashMap<>();
    private final List<Segment> damaged = new ArrayList<>();
    private long firstDamagedIndex = -1;
    private long highestNumber;

    // ---- the index: one slot per stored entry, slot 0 is the entry firstIndex
    private long firstIndex;
    private int count;
    private long[] terms = new long[64];
    private long[] segmentOf = new long[64];
    private long[] offsetOf = new long[64];
    private int[] ordinalOf = new int[64];

    // ---- the configuration at the end of the log, for the header of the next segment
    private ClusterConfig baseConfig;
    private final List<Long> configIndexes = new ArrayList<>();
    private final List<ClusterConfig> configValues = new ArrayList<>();

    // ---- the segment being written
    private LogWriter writer;
    private long activeNumber = -1;
    /** The entries records written to the active segment, by the offset they were written at. */
    private final Map<Long, List<LogEntry>> activeRecords = new HashMap<>();
    private boolean rollDue;
    private boolean failed;

    // ---- the record read last from a sealed segment
    private long cachedSegment = -1;
    private long cachedOffset = -1;
    private List<LogEntry> cachedRecord;

    private RaftLogStore(StorageFileSystem fs, Path directory, long segmentSize, long snapshotIndex,
                         ClusterConfig snapshotConfig) {
        this.fs = fs;
        this.directory = directory;
        this.segmentSize = segmentSize;
        this.firstIndex = snapshotIndex + 1;
        this.baseConfig = snapshotConfig;
    }

    /**
     * Opens the log in {@code directory}, creating the directory if it is missing.
     *
     * @param segmentSize    a segment is closed when it has reached this size
     * @param snapshotIndex  the last index the snapshot of the state machine includes; the log
     *                       must reach back to the entry after it
     * @param snapshotConfig the configuration as of the snapshot
     * @throws UnsupportedFormatException if a segment has an unknown format version
     */
    public static RaftLogStore open(StorageFileSystem fs, Path directory, long segmentSize,
                                    long snapshotIndex, ClusterConfig snapshotConfig) throws IOException {
        fs.createDirectories(directory);
        RaftLogStore store = new RaftLogStore(fs, directory, segmentSize, snapshotIndex, snapshotConfig);
        store.recover(snapshotIndex);
        if (store.firstDamagedIndex == -1) {
            store.startSegment();
        }
        return store;
    }

    // ------------------------------------------------------------------ recovery

    private static String name(long number) {
        return String.format("%010d%s", number, SUFFIX);
    }

    private void recover(long snapshotIndex) throws IOException {
        List<Segment> found = new ArrayList<>();
        for (Path path : fs.list(directory)) {
            String file = path.getFileName().toString();
            if (file.endsWith(SUFFIX) && file.length() == 10 + SUFFIX.length()) {
                found.add(new Segment(Long.parseLong(file.substring(0, 10)), directory.resolve(file)));
            }
        }
        found.sort((a, b) -> Long.compare(a.number, b.number));
        for (Segment segment : found) {
            highestNumber = Math.max(highestNumber, segment.number);
        }
        // A last segment without a header record was created and not written before a crash.
        // A segment is used only after its header is synced, so such a file never held
        // anything that counted. It is removed, and its predecessor is the last segment.
        while (!found.isEmpty() && neverStarted(found.getLast())) {
            fs.delete(found.removeLast().path);
            fs.syncDirectory(directory);
        }
        for (int k = 0; k < found.size(); k++) {
            Segment segment = found.get(k);
            if (firstDamagedIndex != -1) {
                damaged.add(segment);
                continue;
            }
            try {
                replay(segment, k == found.size() - 1 ? null : found.get(k + 1),
                        k == 0 ? -1 : found.get(k - 1).number);
                segments.add(segment);
            } catch (CorruptionException e) {
                // Nothing of a damaged segment is kept: the file is set aside as a whole, and
                // an entry in it could not be read later. What the segments before it hold
                // stands; nothing after that can be vouched for. The index is built again
                // from them: what the damaged segment did to it before the damage showed,
                // such as a cut of the log and the entries after the cut, is undone with that.
                rebuildIndex(found.subList(0, k + 1), snapshotIndex);
                firstDamagedIndex = lastIndex() + 1;
                damaged.add(segment);
            }
        }
        if (firstDamagedIndex == -1 && firstIndex > snapshotIndex + 1) {
            // The segments between the snapshot and the first stored entry are gone.
            firstDamagedIndex = snapshotIndex + 1;
            damaged.addAll(segments);
            segments.clear();
            count = 0;
            firstIndex = snapshotIndex + 1;
            configIndexes.clear();
            configValues.clear();
        }
    }

    /**
     * Builds the index again from the segments before the last of {@code through}, which have
     * been read once without damage; the last is their successor.
     */
    private void rebuildIndex(List<Segment> through, long snapshotIndex) throws IOException {
        firstIndex = snapshotIndex + 1;
        count = 0;
        configIndexes.clear();
        configValues.clear();
        for (int k = 0; k < through.size() - 1; k++) {
            replay(through.get(k), through.get(k + 1), k == 0 ? -1 : through.get(k - 1).number);
        }
    }

    /** Whether the file ends before a complete segment header record, without any sign of damage. */
    private boolean neverStarted(Segment segment) throws IOException {
        boolean[] any = {false};
        try (ReadableFile file = fs.open(segment.path)) {
            LogReader.read(file, segment.path.getFileName().toString(), segment.number, FileKind.RAFT_LOG,
                    false, (offset, payload) -> any[0] = true);
        } catch (CorruptionException e) {
            return false;
        }
        return !any[0];
    }

    /**
     * Reads one segment and applies its records to the index.
     *
     * @param successor      the next segment, or {@code null} for the last one
     * @param expectPrevious the number the header must name as predecessor, or -1 for the
     *                       first segment found (its predecessors were deleted)
     */
    private void replay(Segment segment, Segment successor, long expectPrevious) throws IOException {
        String file = segment.path.getFileName().toString();
        long limit = -1;
        if (successor != null) {
            // The successor says how much of this segment counts. If its header cannot be
            // read, this segment is read as a whole; the successor is found damaged next.
            try (ReadableFile next = fs.open(successor.path)) {
                SegmentHeader header = decodeHeader(LogReader.readRecord(next,
                        successor.path.getFileName().toString(), successor.number, HEADER_RECORD_OFFSET),
                        successor.path.getFileName().toString());
                if (header.previousNumber() == segment.number) {
                    limit = header.previousValidLength();
                }
            } catch (CorruptionException e) {
                limit = -1;
            }
        }
        try (ReadableFile raw = fs.open(segment.path)) {
            long size = raw.size();
            if (limit > size) {
                throw new CorruptionException(file, size, "the segment is shorter than the " + limit
                        + " bytes its successor vouches for");
            }
            ReadableFile view = limit == -1 ? raw : new Prefix(raw, limit);
            boolean[] headerSeen = {false};
            LogReader.Result result = LogReader.read(view, file, segment.number, FileKind.RAFT_LOG,
                    successor != null, (offset, payload) -> {
                        if (payload.length == 0) {
                            throw new CorruptionException(file, offset, "an empty record");
                        }
                        int type = payload[0] & 0xFF;
                        if (!headerSeen[0]) {
                            if (type != RECORD_HEADER) {
                                throw new CorruptionException(file, offset, "the segment has no header record");
                            }
                            headerSeen[0] = true;
                            SegmentHeader header = decodeHeader(payload, file);
                            if (expectPrevious != -1 && (header.previousNumber() != expectPrevious
                                    || header.firstIndex() != lastIndex() + 1)) {
                                throw new CorruptionException(file, offset, "the segment does not follow segment "
                                        + expectPrevious + " at index " + (lastIndex() + 1) + ": " + header);
                            }
                            if (expectPrevious == -1) {
                                firstIndex = header.firstIndex();
                            }
                            return;
                        }
                        switch (type) {
                            case RECORD_ENTRIES -> {
                                List<LogEntry> entries = decodeEntries(payload, file, offset);
                                if (entries.isEmpty() || entries.getFirst().index() != lastIndex() + 1) {
                                    throw new CorruptionException(file, offset, "entries that do not follow index "
                                            + lastIndex());
                                }
                                for (int i = 0; i < entries.size(); i++) {
                                    add(entries.get(i), segment.number, offset, i);
                                }
                            }
                            case RECORD_TRUNCATE -> {
                                long from = decodeTruncate(payload, file, offset);
                                if (from > lastIndex() + 1) {
                                    throw new CorruptionException(file, offset, "a truncation at " + from
                                            + " beyond the log end " + lastIndex());
                                }
                                if (from < firstIndex) {
                                    // The entries it removed were in segments that are deleted
                                    // by now. The log continues at this index.
                                    count = 0;
                                    firstIndex = from;
                                    configIndexes.clear();
                                    configValues.clear();
                                } else {
                                    removeFrom(from);
                                }
                            }
                            default -> throw new CorruptionException(file, offset, "unknown record type " + type);
                        }
                    });
            if (!headerSeen[0]) {
                if (successor != null) {
                    throw new CorruptionException(file, 0, "the segment has no header record");
                }
                // The last segment was created and not written before a crash: it holds nothing.
            }
            segment.validLength = result.validLength();
        }
    }

    /** A view of the first bytes of a file: what a successor vouches for. */
    private record Prefix(ReadableFile file, long length) implements ReadableFile {
        @Override
        public int read(long position, java.nio.ByteBuffer dst) throws IOException {
            if (position >= length) {
                return -1;
            }
            if (dst.remaining() > length - position) {
                java.nio.ByteBuffer limited = dst.duplicate();
                limited.limit(limited.position() + (int) (length - position));
                int n = file.read(position, limited);
                if (n > 0) {
                    dst.position(dst.position() + n);
                }
                return n;
            }
            return file.read(position, dst);
        }

        @Override
        public long size() {
            return length;
        }

        @Override
        public void close() {
            // the owner closes the file
        }
    }

    /** The first index the store cannot vouch for after damage was found, or -1. */
    public long firstDamagedIndex() {
        return firstDamagedIndex;
    }

    /** The names of the damaged segment files that wait to be set aside. */
    public List<String> damagedFiles() {
        return damaged.stream().map(s -> s.path.getFileName().toString()).toList();
    }

    /**
     * Moves the damaged segments into the directory {@value #DAMAGED_DIRECTORY} and makes the
     * store writable. Nothing is deleted: the files stay as evidence. Call this only after
     * the hard state says that the node is in log-recovering mode.
     */
    public void quarantineDamaged() throws IOException {
        if (firstDamagedIndex == -1) {
            throw new IllegalStateException("the log is not damaged");
        }
        Path aside = directory.resolve(DAMAGED_DIRECTORY);
        fs.createDirectories(aside);
        for (Segment segment : damaged) {
            fs.rename(segment.path, aside.resolve(segment.path.getFileName()));
        }
        fs.syncDirectory(aside);
        fs.syncDirectory(directory);
        damaged.clear();
        firstDamagedIndex = -1;
        startSegment();
    }

    // ------------------------------------------------------------------ writing

    private void requireWritable() throws IOException {
        if (failed) {
            throw new IOException("an earlier write to the Raft log failed; the node must stop");
        }
        if (writer == null) {
            throw new IllegalStateException("the log has damaged segments that are not set aside yet");
        }
    }

    /** Starts the segment that takes the next writes. It names how much of its predecessor counts. */
    private void startSegment() throws IOException {
        Segment previous = segments.isEmpty() ? null : segments.getLast();
        Segment segment = new Segment(highestNumber + 1, directory.resolve(name(highestNumber + 1)));
        highestNumber = segment.number;
        WritableFile file = fs.create(segment.path);
        LogWriter next = new LogWriter(file, segment.number, FileKind.RAFT_LOG);
        next.addRecord(encodeHeader(new SegmentHeader(lastIndex() + 1,
                previous == null ? 0 : previous.number, previous == null ? 0 : previous.validLength,
                configAtEnd())));
        next.sync();
        fs.syncDirectory(directory);
        segments.add(segment);
        writer = next;
        activeNumber = segment.number;
        activeRecords.clear();
        rollDue = false;
    }

    /**
     * Appends entries. They are durable after the next {@link #sync}.
     *
     * @param entries entries with consecutive indexes that follow the last stored one
     */
    public void append(List<LogEntry> entries) throws IOException {
        requireWritable();
        if (entries.isEmpty()) {
            return;
        }
        for (int i = 0; i < entries.size(); i++) {
            if (entries.get(i).index() != lastIndex() + 1 + i) {
                throw new IllegalArgumentException("entry " + entries.get(i) + " does not follow index "
                        + (lastIndex() + i));
            }
        }
        failed = true;
        long offset = writer.size();
        writer.addRecord(encodeEntries(entries));
        failed = false;
        List<LogEntry> record = List.copyOf(entries);
        for (int i = 0; i < record.size(); i++) {
            add(record.get(i), activeNumber, offset, i);
        }
        activeRecords.put(offset, record);
        rollDue = writer.size() >= segmentSize;
    }

    /** Removes every entry from {@code from} on, by a record. Durable after the next sync. */
    public void truncateSuffix(long from) throws IOException {
        requireWritable();
        if (from > lastIndex()) {
            return;
        }
        if (from < firstIndex) {
            throw new IllegalArgumentException("truncation at " + from + " before the first stored index "
                    + firstIndex);
        }
        byte[] record = new byte[9];
        record[0] = RECORD_TRUNCATE;
        LittleEndian.putUint64(record, 1, from);
        failed = true;
        writer.addRecord(record);
        failed = false;
        removeFrom(from);
    }

    /** Makes everything written so far durable. A full segment is closed and the next one started. */
    public void sync() throws IOException {
        requireWritable();
        failed = true;
        writer.sync();
        if (rollDue) {
            segments.getLast().validLength = writer.size();
            writer.close();
            writer = null;
            startSegment();
        }
        failed = false;
    }

    /**
     * A sync of the segment being written that runs on another thread while the store takes
     * more appends (docs/spec/node.md, section 1). It covers what was written when it was
     * begun. {@link #run} is the only part that may run on another thread; whoever runs it
     * must make what it records visible to the thread of the store.
     */
    public static final class Sync implements Runnable {
        private final LogWriter writer;
        private final long length;
        private final long lastIndex;
        private final Clock clock;
        private long nanos;
        private Throwable failure;

        private Sync(LogWriter writer, long length, long lastIndex, Clock clock) {
            this.writer = writer;
            this.length = length;
            this.lastIndex = lastIndex;
            this.clock = clock;
        }

        /** Syncs the segment. A failure is kept for {@link #endSync}. */
        @Override
        public void run() {
            long started = clock.nanoTime();
            try {
                writer.syncFile();
            } catch (Throwable e) {
                failure = e;
            }
            nanos = clock.nanoTime() - started;
        }

        /** The last index the sync covers. */
        public long lastIndex() {
            return lastIndex;
        }

        /** How long the sync of the file took. */
        public long nanos() {
            return nanos;
        }
    }

    /**
     * Begins a sync of everything written so far, to be run by {@link Sync#run} on any thread
     * and ended by {@link #endSync}. Until then the store takes appends, and nothing else
     * that writes.
     */
    public Sync beginSync(Clock clock) throws IOException {
        requireWritable();
        return new Sync(writer, writer.size(), lastIndex(), clock);
    }

    /**
     * Ends a sync that has run. A full segment is closed now, after a sync of what was
     * appended since the sync began, and the next one is started.
     *
     * @return the last index that is durable now
     * @throws IOException if the sync failed; that is fatal for the store
     */
    public long endSync(Sync sync) throws IOException {
        if (sync.failure != null) {
            failed = true;
            switch (sync.failure) {
                case IOException e -> throw e;
                case RuntimeException e -> throw e;
                case Error e -> throw e;
                default -> throw new IOException(sync.failure);
            }
        }
        requireWritable();
        if (sync.writer != writer) {
            throw new IllegalStateException("the sync is of another segment");
        }
        writer.synced(sync.length);
        if (rollDue) {
            sync();
            return lastIndex();
        }
        return sync.lastIndex;
    }

    /**
     * Deletes the segments that hold only entries up to {@code through}: a snapshot covers
     * them. Entries up to {@code through} that share a segment with later ones stay.
     */
    public void truncatePrefix(long through) throws IOException {
        requireWritable();
        boolean deleted = false;
        while (segments.size() > 1) {
            Segment oldest = segments.getFirst();
            long next = segments.get(1).number;
            // The first stored entry that lies in a later segment.
            int slot = 0;
            while (slot < count && segmentOf[slot] < next) {
                slot++;
            }
            if (firstIndex + slot - 1 > through) {
                break;
            }
            ReadableFile reader = readers.remove(oldest.number);
            if (reader != null) {
                reader.close();
            }
            if (cachedSegment == oldest.number) {
                cachedSegment = -1;
                cachedRecord = null;
            }
            fs.delete(oldest.path);
            segments.removeFirst();
            dropFirst(slot);
            deleted = true;
        }
        if (deleted) {
            fs.syncDirectory(directory);
        }
    }

    /**
     * Gives up every stored entry: the log starts anew after a snapshot that the stored
     * entries do not lead up to. The next entry to append is the one after
     * {@code snapshotIndex}.
     *
     * <p>The caller records durably that it is about to do this, and repeats it with
     * {@link #deleteSegments} if it finds that record at its next start.
     */
    public void reset(long snapshotIndex, ClusterConfig snapshotConfig) throws IOException {
        requireWritable();
        failed = true;
        close();
        for (int i = segments.size() - 1; i >= 0; i--) {
            fs.delete(segments.get(i).path);
        }
        fs.syncDirectory(directory);
        segments.clear();
        activeRecords.clear();
        activeNumber = -1;
        cachedSegment = -1;
        cachedRecord = null;
        count = 0;
        firstIndex = snapshotIndex + 1;
        baseConfig = snapshotConfig;
        configIndexes.clear();
        configValues.clear();
        startSegment();
        failed = false;
    }

    /**
     * Deletes every segment of a log that is not open. What was set aside as damaged stays.
     */
    public static void deleteSegments(StorageFileSystem fs, Path directory) throws IOException {
        if (!fs.exists(directory)) {
            return;
        }
        List<Path> paths = new ArrayList<>();
        for (Path path : fs.list(directory)) {
            String file = path.getFileName().toString();
            if (file.endsWith(SUFFIX) && file.length() == 10 + SUFFIX.length()) {
                paths.add(path);
            }
        }
        // The newest first: whatever a crash leaves is a log that ends earlier.
        for (int i = paths.size() - 1; i >= 0; i--) {
            fs.delete(paths.get(i));
        }
        fs.syncDirectory(directory);
    }

    /** The bytes the segments of the log take on disk. */
    public long storedBytes() {
        long bytes = 0;
        for (Segment segment : segments) {
            bytes += segment.number == activeNumber && writer != null ? writer.size() : segment.validLength;
        }
        return bytes;
    }

    @Override
    public void close() throws IOException {
        IOException first = null;
        for (ReadableFile reader : readers.values()) {
            try {
                reader.close();
            } catch (IOException e) {
                first = first == null ? e : first;
            }
        }
        readers.clear();
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                first = first == null ? e : first;
            }
            writer = null;
        }
        if (first != null) {
            throw first;
        }
    }

    // ------------------------------------------------------------------ reading

    @Override
    public long firstIndex() {
        return firstIndex;
    }

    @Override
    public long lastIndex() {
        return firstIndex + count - 1;
    }

    @Override
    public long termAt(long index) {
        return terms[slot(index)];
    }

    @Override
    public List<LogEntry> entries(long fromInclusive, long toExclusive, int maxBytes) {
        List<LogEntry> result = new ArrayList<>();
        long bytes = 0;
        for (long index = fromInclusive; index < toExclusive; index++) {
            LogEntry entry = get(index);
            if (!result.isEmpty() && bytes + entry.size() > maxBytes) {
                break;
            }
            bytes += entry.size();
            result.add(entry);
        }
        return result;
    }

    /** The number of segment files of the log. */
    public int segmentCount() {
        return segments.size();
    }

    /** The configuration in force at the end of the stored log. */
    public ClusterConfig configAtEnd() {
        return configValues.isEmpty() ? baseConfig : configValues.getLast();
    }

    private int slot(long index) {
        if (index < firstIndex || index > lastIndex()) {
            throw new IllegalArgumentException("entry " + index + " is not stored: " + firstIndex + ".."
                    + lastIndex());
        }
        return (int) (index - firstIndex);
    }

    private LogEntry get(long index) {
        int slot = slot(index);
        long segment = segmentOf[slot];
        if (segment == activeNumber) {
            return activeRecords.get(offsetOf[slot]).get(ordinalOf[slot]);
        }
        try {
            if (segment != cachedSegment || offsetOf[slot] != cachedOffset) {
                ReadableFile reader = readers.get(segment);
                if (reader == null) {
                    reader = fs.open(directory.resolve(name(segment)));
                    readers.put(segment, reader);
                }
                String file = name(segment);
                cachedRecord = decodeEntries(LogReader.readRecord(reader, file, segment, offsetOf[slot]),
                        file, offsetOf[slot]);
                cachedSegment = segment;
                cachedOffset = offsetOf[slot];
            }
            LogEntry entry = cachedRecord.get(ordinalOf[slot]);
            if (entry.index() != index || entry.term() != terms[slot]) {
                throw new CorruptionException(name(segment), offsetOf[slot], "the record holds " + entry
                        + " where entry " + index + " of term " + terms[slot] + " was written");
            }
            return entry;
        } catch (IOException e) {
            // Damage that appeared after the log was opened. The node must not go on with it.
            throw new java.io.UncheckedIOException(e);
        }
    }

    // ------------------------------------------------------------------ the index

    private void add(LogEntry entry, long segment, long offset, int ordinal) {
        if (count == terms.length) {
            int capacity = count * 2;
            terms = Arrays.copyOf(terms, capacity);
            segmentOf = Arrays.copyOf(segmentOf, capacity);
            offsetOf = Arrays.copyOf(offsetOf, capacity);
            ordinalOf = Arrays.copyOf(ordinalOf, capacity);
        }
        terms[count] = entry.term();
        segmentOf[count] = segment;
        offsetOf[count] = offset;
        ordinalOf[count] = ordinal;
        count++;
        if (entry.type() == EntryType.CONFIG) {
            configIndexes.add(entry.index());
            configValues.add(ClusterConfig.decode(entry.payload()));
        }
    }

    private void removeFrom(long from) {
        if (from > lastIndex()) {
            return;
        }
        count = (int) (from - firstIndex);
        while (!configIndexes.isEmpty() && configIndexes.getLast() >= from) {
            configIndexes.removeLast();
            configValues.removeLast();
        }
    }

    private void dropFirst(int slots) {
        if (slots == 0) {
            return;
        }
        System.arraycopy(terms, slots, terms, 0, count - slots);
        System.arraycopy(segmentOf, slots, segmentOf, 0, count - slots);
        System.arraycopy(offsetOf, slots, offsetOf, 0, count - slots);
        System.arraycopy(ordinalOf, slots, ordinalOf, 0, count - slots);
        count -= slots;
        firstIndex += slots;
    }

    // ------------------------------------------------------------------ records

    public static byte[] encodeHeader(SegmentHeader header) {
        byte[] config = header.config().encode();
        byte[] record = new byte[2 + 8 + 8 + 8 + 4 + config.length];
        record[0] = RECORD_HEADER;
        record[1] = SEGMENT_VERSION;
        LittleEndian.putUint64(record, 2, header.firstIndex());
        LittleEndian.putUint64(record, 10, header.previousNumber());
        LittleEndian.putUint64(record, 18, header.previousValidLength());
        LittleEndian.putInt32(record, 26, config.length);
        System.arraycopy(config, 0, record, 30, config.length);
        return record;
    }

    public static SegmentHeader decodeHeader(byte[] record, String file) throws IOException {
        if (record.length < 30 || record[0] != RECORD_HEADER) {
            throw new CorruptionException(file, HEADER_RECORD_OFFSET, "not a segment header record");
        }
        if (record[1] != SEGMENT_VERSION) {
            throw new UnsupportedFormatException("Raft log segment " + file + " has format version "
                    + (record[1] & 0xFF) + "; this software reads version " + SEGMENT_VERSION);
        }
        int length = LittleEndian.getInt32(record, 26);
        if (length < 0 || length != record.length - 30) {
            throw new CorruptionException(file, HEADER_RECORD_OFFSET, "a segment header of the wrong length");
        }
        try {
            return new SegmentHeader(LittleEndian.getUint64(record, 2), LittleEndian.getUint64(record, 10),
                    LittleEndian.getUint64(record, 18),
                    ClusterConfig.decode(Arrays.copyOfRange(record, 30, record.length)));
        } catch (IllegalArgumentException e) {
            throw new CorruptionException(file, HEADER_RECORD_OFFSET, "a segment header with an invalid configuration");
        }
    }

    public static byte[] encodeEntries(List<LogEntry> entries) {
        int size = 1 + 8 + 4;
        for (LogEntry entry : entries) {
            size += 8 + 1 + 4 + entry.payload().length;
        }
        byte[] record = new byte[size];
        record[0] = RECORD_ENTRIES;
        LittleEndian.putUint64(record, 1, entries.getFirst().index());
        LittleEndian.putInt32(record, 9, entries.size());
        int at = 13;
        for (LogEntry entry : entries) {
            LittleEndian.putUint64(record, at, entry.term());
            record[at + 8] = (byte) entry.type().ordinal();
            LittleEndian.putInt32(record, at + 9, entry.payload().length);
            System.arraycopy(entry.payload(), 0, record, at + 13, entry.payload().length);
            at += 13 + entry.payload().length;
        }
        return record;
    }

    public static List<LogEntry> decodeEntries(byte[] record, String file, long offset) throws IOException {
        if (record.length < 13 || record[0] != RECORD_ENTRIES) {
            throw new CorruptionException(file, offset, "not an entries record");
        }
        long index = LittleEndian.getUint64(record, 1);
        int n = LittleEndian.getInt32(record, 9);
        EntryType[] types = EntryType.values();
        List<LogEntry> entries = new ArrayList<>(Math.max(0, Math.min(n, 1024)));
        int at = 13;
        for (int i = 0; i < n; i++) {
            if (record.length - at < 13) {
                throw new CorruptionException(file, offset, "an entries record that ends early");
            }
            long term = LittleEndian.getUint64(record, at);
            int type = record[at + 8] & 0xFF;
            int length = LittleEndian.getInt32(record, at + 9);
            if (type >= types.length || length < 0 || length > record.length - at - 13 || term < 1
                    || index + i < 1) {
                throw new CorruptionException(file, offset, "an entries record with an invalid entry");
            }
            entries.add(new LogEntry(index + i, term, types[type],
                    Arrays.copyOfRange(record, at + 13, at + 13 + length)));
            at += 13 + length;
        }
        if (n < 0 || at != record.length) {
            throw new CorruptionException(file, offset, "an entries record of the wrong length");
        }
        return entries;
    }

    public static long decodeTruncate(byte[] record, String file, long offset) throws IOException {
        if (record.length != 9) {
            throw new CorruptionException(file, offset, "a truncation record of the wrong length");
        }
        return LittleEndian.getUint64(record, 1);
    }
}
