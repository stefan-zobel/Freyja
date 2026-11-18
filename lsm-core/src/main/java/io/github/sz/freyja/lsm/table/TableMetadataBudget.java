package io.github.sz.freyja.lsm.table;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Limits the memory that the index and filter blocks of open tables take
 * (docs/spec/storage-engine.md, section 4).
 *
 * <p>Every table reader loads its index and filter block when it is opened and reports their
 * size here. When the total exceeds the budget, the readers that were used least recently
 * drop their blocks; a reader loads them again, and verifies them again, when it is next
 * used. The file handle of a reader stays open all the time.
 *
 * <p>The reader that has just loaded its blocks is never asked to drop them, so a budget
 * smaller than one table's blocks still lets every read finish.
 *
 * <p>Safe for concurrent use.
 */
public final class TableMetadataBudget {

    private final long capacity;
    private final Map<SSTableReader, Long> loaded = new IdentityHashMap<>();
    private final AtomicLong clock = new AtomicLong();
    private long used;
    private long drops;

    /** @param capacityBytes the budget in bytes */
    public TableMetadataBudget(long capacityBytes) {
        if (capacityBytes < 0) {
            throw new IllegalArgumentException("negative budget");
        }
        this.capacity = capacityBytes;
    }

    /** Returns a value that grows with every call; readers stamp their uses with it. */
    long tick() {
        return clock.incrementAndGet();
    }

    /** Called by a reader that has loaded {@code bytes} of index and filter blocks. */
    synchronized void loaded(SSTableReader reader, long bytes) {
        Long previous = loaded.put(reader, bytes);
        used += bytes - (previous == null ? 0 : previous);
        while (used > capacity) {
            SSTableReader victim = null;
            for (SSTableReader candidate : loaded.keySet()) {
                if (candidate != reader && (victim == null || candidate.lastUse() < victim.lastUse())) {
                    victim = candidate;
                }
            }
            if (victim == null) {
                return; // only the reader that just loaded is left
            }
            used -= loaded.remove(victim);
            victim.dropMetadata();
            drops++;
        }
    }

    /** Called by a reader that is closed. */
    synchronized void removed(SSTableReader reader) {
        Long bytes = loaded.remove(reader);
        if (bytes != null) {
            used -= bytes;
        }
    }

    /** Returns the budget in bytes. */
    public long capacity() {
        return capacity;
    }

    /** Returns the size of the index and filter blocks that are loaded now. */
    public synchronized long usedBytes() {
        return used;
    }

    /** Returns the number of readers whose blocks are loaded now. */
    public synchronized int loadedCount() {
        return loaded.size();
    }

    /** Returns how often a reader was asked to drop its blocks. */
    public synchronized long drops() {
        return drops;
    }
}
