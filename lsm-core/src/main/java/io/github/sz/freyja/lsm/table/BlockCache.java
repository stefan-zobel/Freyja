package io.github.sz.freyja.lsm.table;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An off-heap cache of verified data blocks (docs/spec/storage-engine.md, section 4).
 *
 * <p>All memory comes from one pool that is allocated when the cache is created and freed
 * when it is closed; no memory is allocated per block. The pool is divided into slabs of
 * 64 KiB. A slab serves one size class (4, 8, 16, 32, or 64 KiB) and is cut into slots of
 * that size. A block occupies one slot of the smallest class that holds it.
 *
 * <p>A block is pinned while a reader uses it. Only unpinned blocks are evicted, the least
 * recently used one of the same size class first. If no slot can be found, the block is not
 * cached and the reader works on its own copy. Blocks larger than 64 KiB are never cached.
 *
 * <p>Safe for concurrent use.
 */
public final class BlockCache implements AutoCloseable {

    private static final int SLAB_SIZE = 64 * 1024;
    private static final int MIN_SLOT_SIZE = 4 * 1024;
    private static final int CLASS_COUNT = 5; // 4, 8, 16, 32, 64 KiB

    private record Key(long fileNumber, long offset) {
    }

    private static final class Entry {
        final Key key;
        final int sizeClass;
        final long slot;   // offset of the slot in the pool
        final int length;  // length of the block
        int pins;

        Entry(Key key, int sizeClass, long slot, int length) {
            this.key = key;
            this.sizeClass = sizeClass;
            this.slot = slot;
            this.length = length;
        }
    }

    /** A pinned block. Closing it releases the pin; the segment must not be used afterward. */
    public final class Pinned implements AutoCloseable {
        private final Entry entry;
        private boolean released;

        private Pinned(Entry entry) {
            this.entry = entry;
        }

        /** Returns the block contents. */
        public MemorySegment segment() {
            if (released) {
                throw new IllegalStateException("block is no longer pinned");
            }
            return pool.asSlice(entry.slot, entry.length);
        }

        @Override
        public void close() {
            synchronized (BlockCache.this) {
                if (!released) {
                    released = true;
                    entry.pins--;
                    pinned--;
                }
            }
        }
    }

    private final Arena arena;
    private final MemorySegment pool;
    private final long capacity;

    private final Map<Key, Entry> entries = new HashMap<>();
    /** Per size class: entries in access order, least recently used first. */
    private final List<LinkedHashMap<Key, Entry>> byClass = new ArrayList<>();
    /** Per size class: offsets of free slots. */
    private final List<ArrayList<Long>> freeSlots = new ArrayList<>();
    private final ArrayList<Long> freeSlabs = new ArrayList<>();

    private long hits;
    private long misses;
    private long evictions;
    private int pinned;
    private boolean closed;

    /**
     * Allocates the pool.
     *
     * @param capacityBytes size of the pool; rounded down to whole slabs of 64 KiB. With less
     *                      than one slab the cache holds nothing.
     */
    public BlockCache(long capacityBytes) {
        if (capacityBytes < 0) {
            throw new IllegalArgumentException("negative capacity");
        }
        long slabs = capacityBytes / SLAB_SIZE;
        this.capacity = slabs * SLAB_SIZE;
        this.arena = Arena.ofShared();
        this.pool = arena.allocate(Math.max(capacity, 1), 8);
        for (long i = slabs - 1; i >= 0; i--) {
            freeSlabs.add(i * SLAB_SIZE);
        }
        for (int c = 0; c < CLASS_COUNT; c++) {
            byClass.add(new LinkedHashMap<>(16, 0.75f, true));
            freeSlots.add(new ArrayList<>());
        }
    }

    /**
     * Returns the cached block and pins it, or returns {@code null} if it is not cached.
     *
     * @param fileNumber the table file the block belongs to
     * @param offset     the offset of the block in that file
     */
    public synchronized Pinned get(long fileNumber, long offset) {
        requireOpen();
        Key key = new Key(fileNumber, offset);
        Entry entry = entries.get(key);
        if (entry == null) {
            misses++;
            return null;
        }
        hits++;
        byClass.get(entry.sizeClass).get(key); // marks it as most recently used
        return pin(entry);
    }

    /**
     * Copies a verified block into the cache and pins it. If the block is already cached,
     * the cached copy is pinned instead.
     *
     * @return the pinned block, or {@code null} if the block cannot be cached now
     */
    public synchronized Pinned insert(long fileNumber, long offset, byte[] contents) {
        requireOpen();
        Key key = new Key(fileNumber, offset);
        Entry existing = entries.get(key);
        if (existing != null) {
            byClass.get(existing.sizeClass).get(key);
            return pin(existing);
        }
        int sizeClass = sizeClassFor(contents.length);
        if (sizeClass < 0) {
            return null;
        }
        Long slot = takeSlot(sizeClass);
        if (slot == null) {
            return null;
        }
        MemorySegment.copy(contents, 0, pool, ValueLayout.JAVA_BYTE, slot, contents.length);
        Entry entry = new Entry(key, sizeClass, slot, contents.length);
        entries.put(key, entry);
        byClass.get(sizeClass).put(key, entry);
        return pin(entry);
    }

    /** Removes every unpinned block of a file, for example before the file is deleted. */
    public synchronized void evictFile(long fileNumber) {
        requireOpen();
        for (int c = 0; c < CLASS_COUNT; c++) {
            Iterator<Entry> it = byClass.get(c).values().iterator();
            while (it.hasNext()) {
                Entry entry = it.next();
                if (entry.key.fileNumber() == fileNumber && entry.pins == 0) {
                    it.remove();
                    entries.remove(entry.key);
                    freeSlots.get(c).add(entry.slot);
                }
            }
        }
    }

    /** Returns the size of the pool in bytes. */
    public long capacity() {
        return capacity;
    }

    /** Returns the number of lookups that found the block. */
    public synchronized long hits() {
        return hits;
    }

    /** Returns the number of lookups that did not find the block. */
    public synchronized long misses() {
        return misses;
    }

    /** Returns the number of blocks evicted to make room. */
    public synchronized long evictions() {
        return evictions;
    }

    /** Returns the number of cached blocks. */
    public synchronized int size() {
        return entries.size();
    }

    /** Returns the number of pins that are currently held. */
    public synchronized int pinnedCount() {
        return pinned;
    }

    /**
     * Frees the pool.
     *
     * @throws IllegalStateException if a block is still pinned; the pool stays allocated
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        if (pinned > 0) {
            throw new IllegalStateException(pinned + " blocks are still pinned");
        }
        closed = true;
        entries.clear();
        arena.close();
    }

    private Pinned pin(Entry entry) {
        entry.pins++;
        pinned++;
        return new Pinned(entry);
    }

    /** Returns the index of the smallest class whose slots hold {@code length} bytes, or -1. */
    private static int sizeClassFor(int length) {
        int slotSize = MIN_SLOT_SIZE;
        for (int c = 0; c < CLASS_COUNT; c++) {
            if (length <= slotSize) {
                return c;
            }
            slotSize <<= 1;
        }
        return -1;
    }

    /** Finds a slot of the class: a free one, a slot of a fresh slab, or an evicted one. */
    private Long takeSlot(int sizeClass) {
        ArrayList<Long> free = freeSlots.get(sizeClass);
        if (free.isEmpty() && !freeSlabs.isEmpty()) {
            long slab = freeSlabs.remove(freeSlabs.size() - 1);
            int slotSize = MIN_SLOT_SIZE << sizeClass;
            for (long slot = slab + SLAB_SIZE - slotSize; slot >= slab; slot -= slotSize) {
                free.add(slot);
            }
        }
        if (!free.isEmpty()) {
            return free.remove(free.size() - 1);
        }
        Iterator<Entry> it = byClass.get(sizeClass).values().iterator();
        while (it.hasNext()) {
            Entry candidate = it.next();
            if (candidate.pins == 0) {
                it.remove();
                entries.remove(candidate.key);
                evictions++;
                return candidate.slot;
            }
        }
        return null;
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("block cache is closed");
        }
    }
}
