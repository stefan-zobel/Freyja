package io.github.stefan_zobel.freyja.lsm;

/**
 * Limits and settings of an engine instance (docs/spec/storage-engine.md, section 4).
 * All limits are enforced. The engine copies the options when it is opened.
 */
public final class EngineOptions {

    private int maxKeySize = 64 * 1024;
    private int maxValueSize = 1024 * 1024;
    private int maxBatchSize = 4 * 1024 * 1024;
    private int maxGroupCommitSize = 8 * 1024 * 1024;
    private long memTableSize = 32L * 1024 * 1024;
    private long blockCacheCapacity = 256L * 1024 * 1024;
    private int levelCount = 7;

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

    private static int requirePositive(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("must be positive: " + value);
        }
        return value;
    }
}
