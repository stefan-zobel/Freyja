package io.github.stefan_zobel.freyja.lsm;

import io.github.stefan_zobel.freyja.common.Clock;
import io.github.stefan_zobel.freyja.common.Metrics;

/**
 * Limits and settings of an engine instance (docs/spec/storage-engine.md, section 4).
 * All limits are enforced. The engine copies the options when it is opened.
 */
public final class EngineOptions {

    /** The default damage listener writes a warning to the system logger. */
    private static final DamageListener DEFAULT_DAMAGE_LISTENER = damage ->
            System.getLogger(LsmEngine.class.getName())
                    .log(System.Logger.Level.WARNING, "damaged file: {0}", damage.getMessage());

    private int maxKeySize = 64 * 1024;
    private int maxValueSize = 1024 * 1024;
    private int maxBatchSize = 4 * 1024 * 1024;
    private int maxGroupCommitSize = 8 * 1024 * 1024;
    private long memTableSize = 32L * 1024 * 1024;
    private long blockCacheCapacity = 256L * 1024 * 1024;
    private int levelCount = 7;
    private int level0CompactionTrigger = 4;
    private long level1TargetSize = 64L * 1024 * 1024;
    private long targetFileSize = 8L * 1024 * 1024;
    private boolean deterministic;
    private int maxImmutableMemTables = 2;
    private int level0SlowdownTrigger = 8;
    private int level0StallTrigger = 12;
    private long writeStallTimeoutMillis = 60_000;
    private Clock clock = Clock.SYSTEM;
    private long tableMetadataBudget = 128L * 1024 * 1024;
    private long maxManifestSize = 64L * 1024 * 1024;
    private long compactionBytesPerSecond;
    private long minFreeSpace = 1024L * 1024 * 1024;
    private long reserveSize = 256L * 1024 * 1024;
    private long scrubPeriodMillis = 7L * 24 * 60 * 60 * 1000;
    private DamageListener damageListener = DEFAULT_DAMAGE_LISTENER;
    private Metrics metrics = Metrics.NONE;

    public EngineOptions() {
    }

    EngineOptions(EngineOptions other) {
        this.maxKeySize = other.maxKeySize;
        this.maxValueSize = other.maxValueSize;
        this.maxBatchSize = other.maxBatchSize;
        this.maxGroupCommitSize = other.maxGroupCommitSize;
        this.memTableSize = other.memTableSize;
        this.blockCacheCapacity = other.blockCacheCapacity;
        this.levelCount = other.levelCount;
        this.level0CompactionTrigger = other.level0CompactionTrigger;
        this.level1TargetSize = other.level1TargetSize;
        this.targetFileSize = other.targetFileSize;
        this.deterministic = other.deterministic;
        this.maxImmutableMemTables = other.maxImmutableMemTables;
        this.level0SlowdownTrigger = other.level0SlowdownTrigger;
        this.level0StallTrigger = other.level0StallTrigger;
        this.writeStallTimeoutMillis = other.writeStallTimeoutMillis;
        this.clock = other.clock;
        this.tableMetadataBudget = other.tableMetadataBudget;
        this.maxManifestSize = other.maxManifestSize;
        this.compactionBytesPerSecond = other.compactionBytesPerSecond;
        this.minFreeSpace = other.minFreeSpace;
        this.reserveSize = other.reserveSize;
        this.scrubPeriodMillis = other.scrubPeriodMillis;
        this.damageListener = other.damageListener;
        this.metrics = other.metrics;
    }

    /** Sets the largest key size in bytes. Default: 64 KiB. */
    public EngineOptions maxKeySize(int bytes) {
        this.maxKeySize = requirePositive(bytes);
        return this;
    }

    public int maxKeySize() {
        return maxKeySize;
    }

    /** Sets the largest value size in bytes. Default: 1 MiB. */
    public EngineOptions maxValueSize(int bytes) {
        this.maxValueSize = requirePositive(bytes);
        return this;
    }

    public int maxValueSize() {
        return maxValueSize;
    }

    /** Sets the largest encoded size of one write batch in bytes. Default: 4 MiB. */
    public EngineOptions maxBatchSize(int bytes) {
        this.maxBatchSize = requirePositive(bytes);
        return this;
    }

    public int maxBatchSize() {
        return maxBatchSize;
    }

    /**
     * Sets how many bytes of queued batches one group commit takes at most. A single batch
     * is always taken, whatever its size. Default: 8 MiB.
     */
    public EngineOptions maxGroupCommitSize(int bytes) {
        this.maxGroupCommitSize = requirePositive(bytes);
        return this;
    }

    public int maxGroupCommitSize() {
        return maxGroupCommitSize;
    }

    /**
     * Sets the size at which the active MemTable is flushed to a table file. Default: 32 MiB.
     */
    public EngineOptions memTableSize(long bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("must be positive: " + bytes);
        }
        this.memTableSize = bytes;
        return this;
    }

    public long memTableSize() {
        return memTableSize;
    }

    /**
     * Sets the size of the off-heap block cache, which is allocated when the engine opens.
     * Zero disables the cache. Default: 256 MiB.
     */
    public EngineOptions blockCacheCapacity(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("must not be negative: " + bytes);
        }
        this.blockCacheCapacity = bytes;
        return this;
    }

    public long blockCacheCapacity() {
        return blockCacheCapacity;
    }

    /** Sets the number of levels, between 2 and 16. Default: 7. */
    public EngineOptions levelCount(int levels) {
        if (levels < 2 || levels > 16) {
            throw new IllegalArgumentException("level count must be between 2 and 16: " + levels);
        }
        this.levelCount = levels;
        return this;
    }

    public int levelCount() {
        return levelCount;
    }

    /**
     * Sets the number of level-0 files at which level 0 is compacted into level 1.
     * Default: 4.
     */
    public EngineOptions level0CompactionTrigger(int files) {
        this.level0CompactionTrigger = requirePositive(files);
        return this;
    }

    public int level0CompactionTrigger() {
        return level0CompactionTrigger;
    }

    /**
     * Sets the total size of level 1 above which it is compacted into level 2. The target
     * of every deeper level is ten times the target of the level above it. Default: 64 MiB.
     */
    public EngineOptions level1TargetSize(long bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("must be positive: " + bytes);
        }
        this.level1TargetSize = bytes;
        return this;
    }

    public long level1TargetSize() {
        return level1TargetSize;
    }

    /** Returns the size above which {@code level} (1 or deeper) is compacted. */
    public long levelTargetSize(int level) {
        long target = level1TargetSize;
        for (int i = 1; i < level; i++) {
            target = (target > Long.MAX_VALUE / 10) ? Long.MAX_VALUE : target * 10;
        }
        return target;
    }

    /**
     * Sets the size at which a compaction starts a new output file. A file is only split
     * between two user keys. Default: 8 MiB.
     */
    public EngineOptions targetFileSize(long bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("must be positive: " + bytes);
        }
        this.targetFileSize = bytes;
        return this;
    }

    public long targetFileSize() {
        return targetFileSize;
    }

    /**
     * Selects deterministic mode. In this mode the engine starts no thread: a flush runs in
     * the write that fills the MemTable, and compaction runs only when the caller asks for
     * it or when a write would otherwise stall. All randomized tests and both simulations
     * use it. Default: off; a background thread flushes and compacts.
     */
    public EngineOptions deterministic(boolean on) {
        this.deterministic = on;
        return this;
    }

    public boolean deterministic() {
        return deterministic;
    }

    /**
     * Sets how many full MemTables may wait for their flush. Writes stall when the limit is
     * reached. Default: 2.
     */
    public EngineOptions maxImmutableMemTables(int count) {
        this.maxImmutableMemTables = requirePositive(count);
        return this;
    }

    public int maxImmutableMemTables() {
        return maxImmutableMemTables;
    }

    /** Sets the number of level-0 files from which every write is delayed a little. Default: 8. */
    public EngineOptions level0SlowdownTrigger(int files) {
        this.level0SlowdownTrigger = requirePositive(files);
        return this;
    }

    public int level0SlowdownTrigger() {
        return level0SlowdownTrigger;
    }

    /** Sets the number of level-0 files at which writes stall. Default: 12. */
    public EngineOptions level0StallTrigger(int files) {
        this.level0StallTrigger = requirePositive(files);
        return this;
    }

    public int level0StallTrigger() {
        return level0StallTrigger;
    }

    /**
     * Sets how long a stalled write waits before it fails with
     * {@link WriteStalledException}. Default: 60 seconds.
     */
    public EngineOptions writeStallTimeoutMillis(long millis) {
        if (millis < 0) {
            throw new IllegalArgumentException("must not be negative: " + millis);
        }
        this.writeStallTimeoutMillis = millis;
        return this;
    }

    public long writeStallTimeoutMillis() {
        return writeStallTimeoutMillis;
    }

    /** Sets the time source. Default: the clock of the machine. */
    public EngineOptions clock(Clock clock) {
        this.clock = java.util.Objects.requireNonNull(clock);
        return this;
    }

    public Clock clock() {
        return clock;
    }

    /**
     * Sets the memory budget for the index and filter blocks of open tables. Tables that
     * were not used recently drop these blocks when it is exceeded. Default: 128 MiB.
     */
    public EngineOptions tableMetadataBudget(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("must not be negative: " + bytes);
        }
        this.tableMetadataBudget = bytes;
        return this;
    }

    public long tableMetadataBudget() {
        return tableMetadataBudget;
    }

    /**
     * Sets the size at which the manifest is replaced by a new one that holds only the
     * current state. Default: 64 MiB.
     */
    public EngineOptions maxManifestSize(long bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("must be positive: " + bytes);
        }
        this.maxManifestSize = bytes;
        return this;
    }

    public long maxManifestSize() {
        return maxManifestSize;
    }

    /**
     * Limits the bytes per second that compaction writes. The limit does not apply while
     * writes are held back, and not in deterministic mode. Default: 0, no limit.
     */
    public EngineOptions compactionBytesPerSecond(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("must not be negative: " + bytes);
        }
        this.compactionBytesPerSecond = bytes;
        return this;
    }

    public long compactionBytesPerSecond() {
        return compactionBytesPerSecond;
    }

    /**
     * Sets the free space below which the engine rejects writes with
     * {@link DiskFullException}. It stays readable, and flushes and compactions continue.
     * Zero disables the check. Default: 1 GiB.
     */
    public EngineOptions minFreeSpace(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("must not be negative: " + bytes);
        }
        this.minFreeSpace = bytes;
        return this;
    }

    public long minFreeSpace() {
        return minFreeSpace;
    }

    /**
     * Sets the size of the {@code RESERVE} file. The engine deletes the file when a flush, a
     * compaction, or a manifest write runs out of space, and creates it again when space is
     * available. Zero means no reserve. Default: 256 MiB.
     */
    public EngineOptions reserveSize(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("must not be negative: " + bytes);
        }
        this.reserveSize = bytes;
        return this;
    }

    public long reserveSize() {
        return reserveSize;
    }

    /**
     * Sets the period within which the background thread reads and verifies every block of
     * every live table once. Zero disables scrubbing. Default: 7 days.
     */
    public EngineOptions scrubPeriodMillis(long millis) {
        if (millis < 0) {
            throw new IllegalArgumentException("must not be negative: " + millis);
        }
        this.scrubPeriodMillis = millis;
        return this;
    }

    public long scrubPeriodMillis() {
        return scrubPeriodMillis;
    }

    /** Sets who is told about damaged files. Default: a warning in the system logger. */
    public EngineOptions damageListener(DamageListener listener) {
        this.damageListener = java.util.Objects.requireNonNull(listener);
        return this;
    }

    public DamageListener damageListener() {
        return damageListener;
    }

    /** Sets the sink for the engine's figures. Default: none. */
    public EngineOptions metrics(Metrics metrics) {
        this.metrics = java.util.Objects.requireNonNull(metrics);
        return this;
    }

    public Metrics metrics() {
        return metrics;
    }

    private static int requirePositive(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("must be positive: " + value);
        }
        return value;
    }
}
