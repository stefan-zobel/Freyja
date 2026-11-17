package io.github.stefan_zobel.freyja.lsm.table;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.common.testing.Seeds;
import io.github.stefan_zobel.freyja.lsm.fs.MemoryFileSystem;
import io.github.stefan_zobel.freyja.lsm.table.SSTableReader.TableIterator;

/** Tests of the off-heap block cache and of the table reader that uses it. */
class BlockCacheTest {

    private static final int SLAB = 64 * 1024;

    private static byte[] block(int length, int salt) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (i * 13 + salt);
        }
        return bytes;
    }

    private static byte[] contents(BlockCache.Pinned pinned) {
        return pinned.segment().toArray(ValueLayout.JAVA_BYTE);
    }

    @Test
    void anInsertedBlockIsFoundWithItsExactContents() {
        try (BlockCache cache = new BlockCache(4 * SLAB)) {
            assertNull(cache.get(1, 0));
            assertEquals(1, cache.misses());

            byte[] data = block(3000, 1);
            try (BlockCache.Pinned inserted = cache.insert(1, 0, data)) {
                assertArrayEquals(data, contents(inserted));
                assertEquals(1, cache.pinnedCount());
            }
            assertEquals(0, cache.pinnedCount());
            try (BlockCache.Pinned found = cache.get(1, 0)) {
                assertNotNull(found);
                assertArrayEquals(data, contents(found));
            }
            assertEquals(1, cache.hits());
            // Same offset in another file, and another offset in the same file, are other blocks.
            assertNull(cache.get(2, 0));
            assertNull(cache.get(1, 1));
        }
    }

    @Test
    void blocksOfEverySizeClassRoundTrip() {
        try (BlockCache cache = new BlockCache(16 * SLAB)) {
            int[] lengths = {0, 1, 4095, 4096, 4097, 8192, 8193, 16384, 16385, 32768, 32769, 65536};
            for (int i = 0; i < lengths.length; i++) {
                byte[] data = block(lengths[i], i);
                try (BlockCache.Pinned pinned = cache.insert(7, i, data)) {
                    assertNotNull(pinned, "length " + lengths[i]);
                    assertArrayEquals(data, contents(pinned));
                }
            }
            for (int i = 0; i < lengths.length; i++) {
                try (BlockCache.Pinned pinned = cache.get(7, i)) {
                    assertArrayEquals(block(lengths[i], i), contents(pinned), "length " + lengths[i]);
                }
            }
            // A block larger than the largest class is not cached.
            assertNull(cache.insert(7, 99, new byte[65537]));
        }
    }

    @Test
    void insertingTheSameBlockTwiceKeepsOneCopy() {
        try (BlockCache cache = new BlockCache(SLAB)) {
            byte[] data = block(100, 3);
            try (BlockCache.Pinned first = cache.insert(1, 5, data);
                 BlockCache.Pinned second = cache.insert(1, 5, data)) {
                assertEquals(1, cache.size());
                assertEquals(2, cache.pinnedCount());
                assertArrayEquals(contents(first), contents(second));
            }
        }
    }

    @Test
    void theLeastRecentlyUsedUnpinnedBlockIsEvicted() {
        // One slab holds 16 blocks of the smallest class.
        try (BlockCache cache = new BlockCache(SLAB)) {
            for (int i = 0; i < 16; i++) {
                cache.insert(1, i, block(4096, i)).close();
            }
            assertEquals(16, cache.size());
            cache.get(1, 0).close(); // block 0 is now the most recently used

            cache.insert(1, 100, block(4096, 100)).close();
            assertEquals(16, cache.size());
            assertEquals(1, cache.evictions());
            assertNull(cache.get(1, 1), "block 1 was the least recently used");
            try (BlockCache.Pinned kept = cache.get(1, 0)) {
                assertArrayEquals(block(4096, 0), contents(kept));
            }
        }
    }

    @Test
    void pinnedBlocksAreNeverEvicted() {
        try (BlockCache cache = new BlockCache(SLAB)) {
            List<BlockCache.Pinned> pins = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                pins.add(cache.insert(1, i, block(4096, i)));
            }
            // Every slot is pinned: a new block cannot be cached, and nothing is lost.
            assertNull(cache.insert(1, 100, block(4096, 100)));
            assertEquals(0, cache.evictions());
            for (int i = 0; i < 16; i++) {
                assertArrayEquals(block(4096, i), contents(pins.get(i)));
            }
            pins.get(3).close();
            try (BlockCache.Pinned added = cache.insert(1, 100, block(4096, 100))) {
                assertNotNull(added);
                assertNull(cache.get(1, 3), "the only unpinned block made room");
            }
            for (BlockCache.Pinned pin : pins) {
                pin.close();
            }
            assertEquals(0, cache.pinnedCount());
        }
    }

    @Test
    void aReleasedPinCannotBeUsedAndCanBeClosedTwice() {
        try (BlockCache cache = new BlockCache(SLAB)) {
            BlockCache.Pinned pinned = cache.insert(1, 0, block(10, 1));
            pinned.close();
            pinned.close();
            assertEquals(0, cache.pinnedCount());
            assertThrows(IllegalStateException.class, pinned::segment);
        }
    }

    @Test
    void theBlocksOfAFileCanBeEvicted() {
        try (BlockCache cache = new BlockCache(4 * SLAB)) {
            cache.insert(1, 0, block(100, 1)).close();
            cache.insert(1, 9, block(5000, 2)).close();
            cache.insert(2, 0, block(100, 3)).close();
            BlockCache.Pinned inUse = cache.insert(1, 50, block(100, 4));

            cache.evictFile(1);
            assertNull(cache.get(1, 0));
            assertNull(cache.get(1, 9));
            try (BlockCache.Pinned other = cache.get(2, 0)) {
                assertNotNull(other, "blocks of other files stay");
            }
            assertArrayEquals(block(100, 4), contents(inUse), "a pinned block stays");
            inUse.close();
            cache.evictFile(1);
            assertNull(cache.get(1, 50), "once unpinned, the block can be evicted");
        }
    }

    @Test
    void aCacheWithoutCapacityCachesNothing() {
        try (BlockCache cache = new BlockCache(0)) {
            assertEquals(0, cache.capacity());
            assertNull(cache.insert(1, 0, block(10, 1)));
            assertNull(cache.get(1, 0));
        }
        try (BlockCache cache = new BlockCache(SLAB - 1)) {
            assertEquals(0, cache.capacity());
        }
        assertThrows(IllegalArgumentException.class, () -> new BlockCache(-1));
    }

    @Test
    void closingWithPinnedBlocksIsRefused() {
        BlockCache cache = new BlockCache(SLAB);
        BlockCache.Pinned pinned = cache.insert(1, 0, block(10, 1));
        assertThrows(IllegalStateException.class, cache::close);
        pinned.close();
        cache.close();
        cache.close();
        assertThrows(IllegalStateException.class, () -> cache.get(1, 0));
    }

    @Test
    void randomUseMatchesAModelAndNeverCorruptsABlock() {
        Seeds.forEach(BlockCacheTest.class, "block-cache", 40, seed -> {
            Random random = new Random(seed);
            try (BlockCache cache = new BlockCache((long) SLAB * (1 + random.nextInt(6)))) {
                Map<Long, byte[]> known = new HashMap<>();   // every block ever offered
                Map<Long, BlockCache.Pinned> pins = new HashMap<>();
                for (int op = 0; op < 5000; op++) {
                    long id = random.nextInt(120);
                    switch (random.nextInt(4)) {
                        case 0 -> {
                            byte[] data = known.computeIfAbsent(id,
                                    k -> block(random.nextInt(4) == 0 ? random.nextInt(40_000) : random.nextInt(4096), (int) id));
                            BlockCache.Pinned pinned = cache.insert(3, id, data);
                            if (pinned != null) {
                                assertArrayEquals(data, contents(pinned));
                                BlockCache.Pinned old = pins.put(id, pinned);
                                if (old != null) {
                                    old.close();
                                }
                            }
                        }
                        case 1 -> {
                            BlockCache.Pinned pinned = cache.get(3, id);
                            if (pinned != null) {
                                assertArrayEquals(known.get(id), contents(pinned));
                                pinned.close();
                            }
                        }
                        case 2 -> {
                            BlockCache.Pinned pinned = pins.remove(id);
                            if (pinned != null) {
                                // A pinned block is still intact, whatever happened meanwhile.
                                assertArrayEquals(known.get(id), contents(pinned));
                                pinned.close();
                            }
                        }
                        default -> {
                            if (random.nextInt(200) == 0) {
                                cache.evictFile(3);
                            }
                        }
                    }
                    assertEquals(pins.size(), cache.pinnedCount());
                }
                for (Map.Entry<Long, BlockCache.Pinned> pin : pins.entrySet()) {
                    assertArrayEquals(known.get(pin.getKey()), contents(pin.getValue()));
                    pin.getValue().close();
                }
            }
        });
    }

    // ---- the table reader with a cache -----------------------------------------------------

    @Test
    void aTableReaderWithACacheReturnsTheSameEntriesAndReleasesItsPins() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        Path dir = Path.of("cache-test").toAbsolutePath();
        fs.createDirectories(dir);
        Path path = dir.resolve("000004.sst");
        List<byte[]> keys = new ArrayList<>();
        try (SSTableWriter writer = new SSTableWriter(fs, path, 4)) {
            for (int i = 0; i < 500; i++) {
                byte[] key = InternalKey.encode(String.format("key-%05d", i).getBytes(), 1000 - i, InternalKey.TYPE_VALUE);
                keys.add(key);
                writer.add(key, block(150, i));
            }
            writer.finish();
        }

        // A cache of one slab is far smaller than the table: blocks are evicted all the time.
        try (BlockCache cache = new BlockCache(SLAB)) {
            SSTableReader reader = SSTableReader.open(fs, path, 4, cache);
            assertTrue(reader.dataBlocks().size() > 16);
            for (int pass = 0; pass < 2; pass++) {
                try (TableIterator it = reader.iterator()) {
                    int i = 0;
                    for (it.seekToFirst(); it.valid(); it.next(), i++) {
                        assertArrayEquals(keys.get(i), it.key());
                        assertArrayEquals(block(150, i), it.value());
                        assertTrue(cache.pinnedCount() <= 1, "an iterator pins one block at a time");
                    }
                    assertEquals(500, i);
                }
                assertEquals(0, cache.pinnedCount());
            }
            assertTrue(cache.evictions() > 0);

            // Two iterators on the same block share the cached copy.
            try (TableIterator first = reader.iterator(); TableIterator second = reader.iterator()) {
                first.seek(keys.get(250));
                second.seek(keys.get(250));
                assertArrayEquals(first.value(), second.value());
                assertEquals(2, cache.pinnedCount());
            }
            for (int i = 0; i < 500; i += 7) {
                byte[] userKey = InternalKey.userKey(keys.get(i));
                assertArrayEquals(block(150, i), reader.get(userKey, Long.MAX_VALUE).value());
            }
            assertEquals(0, cache.pinnedCount());
            reader.release();
        }
        assertEquals(0, fs.openHandleCount());
    }
}
