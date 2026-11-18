package io.github.stefan_zobel.freyja.lsm.table;

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.zip.CRC32C;

import io.github.stefan_zobel.freyja.common.Checksums;
import io.github.stefan_zobel.freyja.common.DecodeException;
import io.github.stefan_zobel.freyja.common.KeyHash;
import io.github.stefan_zobel.freyja.common.LittleEndian;
import io.github.stefan_zobel.freyja.lsm.CorruptionException;
import io.github.stefan_zobel.freyja.lsm.UnsupportedFormatException;
import io.github.stefan_zobel.freyja.lsm.fs.ReadableFile;
import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;

/**
 * Reads one table file (docs/spec/storage-format.md, section 5).
 *
 * <p>Opening verifies the footer, the index block, and the filter block. Every data block is
 * verified when it is read. A unit that fails verification raises
 * {@link CorruptionException}; its contents are never returned.
 *
 * <p>The reader is reference-counted (docs/spec/storage-engine.md, section 2). It starts
 * with one reference, owned by the caller of {@link #open}. Every iterator holds one more
 * until it is closed. The file handle is closed when the count reaches zero, and only then
 * may the file be deleted.
 *
 * <p>Safe for concurrent use; each iterator belongs to one thread.
 */
public final class SSTableReader {

    /**
     * One data block as listed in the index.
     *
     * @param lastKey the last internal key of the block
     * @param handle  the location of the block
     */
    public record DataBlockInfo(byte[] lastKey, BlockHandle handle) {
    }

    private final ReadableFile file;
    private final String name;
    private final long fileNumber;
    private final long fileSize;
    private final long footerOffset;
    private final BlockCache cache; // may be null
    private final AtomicInteger references = new AtomicInteger(1);
    private volatile Runnable whenUnused; // run once after the last reference is released
    private volatile Consumer<CorruptionException> damageReporter;

    /**
     * The index and filter block of the table.
     *
     * @param index  the index block
     * @param filter the Bloom filter
     * @param bytes  the size of both blocks
     */
    private record Metadata(Block index, BloomFilter filter, long bytes) {
    }

    private final TableMetadataBudget budget; // may be null
    private int formatVersion;
    private BlockHandle filterHandle;
    private BlockHandle indexHandle;
    private volatile Metadata metadata; // null while the blocks are not loaded
    private volatile long lastUse;

    private SSTableReader(ReadableFile file, String name, long fileNumber, long fileSize,
                          BlockCache cache, TableMetadataBudget budget) {
        this.cache = cache;
        this.budget = budget;
        this.file = file;
        this.name = name;
        this.fileNumber = fileNumber;
        this.fileSize = fileSize;
        this.footerOffset = fileSize - TableFormat.FOOTER_LENGTH;
    }

    /**
     * Opens and verifies a table.
     *
     * @param fileNumber the number under which the file was written; a table opened under
     *                   another number fails its checksums
     * @throws CorruptionException        if the footer, index, or filter fails verification
     * @throws UnsupportedFormatException if the table uses an unknown format version
     */
    public static SSTableReader open(StorageFileSystem fs, Path path, long fileNumber)
            throws IOException {
        return open(fs, path, fileNumber, null);
    }

    /**
     * Opens and verifies a table whose data blocks are kept in {@code cache}.
     *
     * @param cache the block cache, or {@code null} to read every block from the file
     * @see #open(StorageFileSystem, Path, long)
     */
    public static SSTableReader open(StorageFileSystem fs, Path path, long fileNumber,
                                     BlockCache cache) throws IOException {
        return open(fs, path, fileNumber, cache, null);
    }

    /**
     * Opens and verifies a table whose index and filter block count against {@code budget}.
     * The reader may drop these blocks while it is not used and loads them again on demand.
     *
     * @param cache  the block cache, or {@code null}
     * @param budget the budget for index and filter blocks, or {@code null} for no limit
     * @see #open(StorageFileSystem, Path, long)
     */
    public static SSTableReader open(StorageFileSystem fs, Path path, long fileNumber,
                                     BlockCache cache, TableMetadataBudget budget) throws IOException {
        ReadableFile file = fs.open(path);
        try {
            SSTableReader reader =
                    new SSTableReader(file, path.toString(), fileNumber, file.size(), cache, budget);
            reader.load();
            return reader;
        } catch (Throwable t) {
            try {
                file.close();
            } catch (IOException e) {
                t.addSuppressed(e);
            }
            throw t;
        }
    }

    private void load() throws IOException {
        readFooter();
        metadata(); // opening verifies the index and the filter block
    }

    /** Reads and verifies the footer and takes the block handles and the version from it. */
    private void readFooter() throws IOException {
        if (fileSize < TableFormat.FOOTER_LENGTH) {
            throw damage(0, "file of " + fileSize + " bytes has no footer");
        }
        byte[] footer = new byte[TableFormat.FOOTER_LENGTH];
        readFully(footerOffset, footer, "footer");

        long magic = LittleEndian.getUint64(footer, 40);
        if ((magic & ~TableFormat.VERSION_MASK) != (TableFormat.MAGIC & ~TableFormat.VERSION_MASK)) {
            throw damage(footerOffset,
                    "bad magic number 0x" + Long.toHexString(magic));
        }
        CRC32C crc = new CRC32C();
        crc.update(footer, 0, TableFormat.FOOTER_CHECKSUM_OFFSET);
        crc.update(footer, 40, 8);
        int expected = Checksums.finishWithContext(crc, fileNumber, footerOffset);
        if (expected != LittleEndian.getInt32(footer, TableFormat.FOOTER_CHECKSUM_OFFSET)) {
            throw damage(footerOffset, "footer checksum mismatch");
        }
        formatVersion = (int) (magic & TableFormat.VERSION_MASK);
        if (formatVersion != TableFormat.FORMAT_VERSION) {
            throw new UnsupportedFormatException("table " + name + " has format version "
                    + formatVersion + "; this release reads version " + TableFormat.FORMAT_VERSION);
        }

        filterHandle = BlockHandle.decode(footer, 0);
        indexHandle = BlockHandle.decode(footer, 16);
    }

    /** Returns the index and filter block, loading and verifying them if necessary. */
    private Metadata metadata() throws IOException {
        Metadata current = metadata;
        if (current == null) {
            current = loadMetadata();
        }
        if (budget != null) {
            lastUse = budget.tick();
        }
        return current;
    }

    private synchronized Metadata loadMetadata() throws IOException {
        Metadata current = metadata;
        if (current != null) {
            return current;
        }
        byte[] indexContents = readBlock(indexHandle);
        byte[] filterContents = readBlock(filterHandle);
        Block index;
        BloomFilter filter;
        try {
            index = new Block(indexContents);
        } catch (DecodeException e) {
            throw damage(indexHandle.offset(), "malformed index block", e);
        }
        try {
            filter = new BloomFilter(filterContents);
        } catch (DecodeException e) {
            throw damage(filterHandle.offset(), "malformed filter block", e);
        }
        current = new Metadata(index, filter, (long) indexContents.length + filterContents.length);
        metadata = current;
        if (budget != null) {
            lastUse = budget.tick();
            budget.loaded(this, current.bytes());
        }
        return current;
    }

    /** Called by the budget: forget the index and filter block until the next use. */
    void dropMetadata() {
        metadata = null;
    }

    /** Returns the stamp of the last use, for the budget. */
    long lastUse() {
        return lastUse;
    }

    /** Returns whether the index and filter block are in memory now. */
    public boolean metadataLoaded() {
        return metadata != null;
    }

    // ---- damage ----------------------------------------------------------------------------

    /**
     * Registers who is told about every piece of damage this reader finds, in addition to
     * the exception that the failing read throws.
     */
    public void onDamage(Consumer<CorruptionException> reporter) {
        this.damageReporter = reporter;
    }

    private CorruptionException damage(long offset, String detail) {
        return damage(offset, detail, null);
    }

    /** Builds the exception for damage in this table and reports it. */
    private CorruptionException damage(long offset, String detail, Throwable cause) {
        CorruptionException damage = (cause == null)
                ? new CorruptionException(name, offset, detail)
                : new CorruptionException(name, offset, detail, cause);
        Consumer<CorruptionException> reporter = damageReporter;
        if (reporter != null) {
            reporter.accept(damage);
        }
        return damage;
    }

    /**
     * Reads the whole table and verifies it: the checksum of every block, the structure of
     * every block, and the order of all keys. Nothing is taken from the block cache.
     *
     * @param onBytes told the size of each block before it is read, for I/O rate limiting;
     *                may be {@code null}
     * @return the number of entries in the table
     * @throws CorruptionException at the first damage found
     */
    public long verify(LongConsumer onBytes) throws IOException {
        retain();
        try {
            // The stored footer, index block, and filter block, whether they are in memory
            // or not.
            readFooter();
            Block index;
            try {
                index = new Block(readBlock(indexHandle));
                new BloomFilter(readBlock(filterHandle));
            } catch (DecodeException e) {
                throw damage(indexHandle.offset(), "malformed index or filter block", e);
            }
            long entries = 0;
            byte[] previousKey = null;
            Block.Iterator blocks = index.iterator(InternalKey.COMPARATOR);
            try {
                for (blocks.seekToFirst(); blocks.valid(); blocks.next()) {
                    BlockHandle handle = decodeHandle(blocks.value());
                    if (onBytes != null) {
                        onBytes.accept(handle.size() + TableFormat.BLOCK_TRAILER_LENGTH);
                    }
                    byte[] lastOfBlock = null;
                    Block.Iterator it;
                    try {
                        it = new Block(readBlock(handle)).iterator(InternalKey.COMPARATOR);
                        for (it.seekToFirst(); it.valid(); it.next()) {
                            byte[] key = it.key();
                            if (!InternalKey.isWellFormed(key) || InternalKey.userKeyLength(key) == 0) {
                                throw damage(handle.offset(), "malformed internal key");
                            }
                            if (previousKey != null && InternalKey.compare(previousKey, key) >= 0) {
                                throw damage(handle.offset(), "keys are not in ascending order");
                            }
                            previousKey = key;
                            lastOfBlock = key;
                            it.value();
                            entries++;
                        }
                    } catch (DecodeException e) {
                        throw damage(handle.offset(), "malformed data block", e);
                    }
                    if (lastOfBlock == null || InternalKey.compare(lastOfBlock, blocks.key()) != 0) {
                        throw damage(handle.offset(), "the index does not name the last key of the block");
                    }
                }
            } catch (DecodeException e) {
                throw damage(indexHandle.offset(), "malformed index block", e);
            }
            if (entries == 0) {
                throw damage(0, "the table holds no entry");
            }
            return entries;
        } finally {
            release();
        }
    }

    // ---- reference counting ----------------------------------------------------------------

    /**
     * Takes one more reference.
     *
     * @throws IllegalStateException if the reader was already released completely
     */
    public void retain() {
        while (true) {
            int current = references.get();
            if (current <= 0) {
                throw new IllegalStateException("table " + name + " is closed");
            }
            if (references.compareAndSet(current, current + 1)) {
                return;
            }
        }
    }

    /** Gives one reference back. The file handle is closed with the last one. */
    public void release() throws IOException {
        int remaining = references.decrementAndGet();
        if (remaining < 0) {
            references.incrementAndGet();
            throw new IllegalStateException("table " + name + " released too often");
        }
        if (remaining == 0) {
            if (budget != null) {
                budget.removed(this);
            }
            try {
                file.close();
            } finally {
                Runnable action = whenUnused;
                if (action != null) {
                    action.run();
                }
            }
        }
    }

    /**
     * Registers an action that runs once, right after the last reference is released and
     * the file handle is closed. The engine uses it to delete a table that is no longer
     * part of any version. The caller must hold a reference while it registers the action.
     */
    public void whenUnused(Runnable action) {
        this.whenUnused = action;
    }

    /** Returns the current number of references. */
    public int referenceCount() {
        return references.get();
    }

    // ---- reading ---------------------------------------------------------------------------

    /**
     * Returns {@code false} only if the table certainly holds no entry for {@code userKey}.
     */
    public boolean mayContain(byte[] userKey) throws IOException {
        return metadata().filter().mayContain(KeyHash.hash(userKey));
    }

    /**
     * Looks up the newest entry of {@code userKey} whose sequence number is at most
     * {@code snapshotSequence}.
     */
    public Lookup get(byte[] userKey, long snapshotSequence) throws IOException {
        if (!mayContain(userKey)) {
            return Lookup.NOT_FOUND;
        }
        try (TableIterator it = iterator()) {
            it.seek(InternalKey.seekKey(userKey, snapshotSequence));
            if (!it.valid()) {
                return Lookup.NOT_FOUND;
            }
            byte[] key = it.key();
            if (!InternalKey.hasUserKey(key, userKey)) {
                return Lookup.NOT_FOUND;
            }
            return Lookup.of(key, it.value());
        }
    }

    /**
     * Returns a new iterator, positioned before the first entry. It holds a reference to
     * this reader and must be closed.
     */
    public TableIterator iterator() throws IOException {
        Block index = metadata().index();
        retain();
        return new TableIterator(index);
    }

    // ---- description, for tools ------------------------------------------------------------

    public long fileNumber() {
        return fileNumber;
    }

    public long fileSize() {
        return fileSize;
    }

    public int formatVersion() {
        return formatVersion;
    }

    public BlockHandle filterHandle() {
        return filterHandle;
    }

    public BlockHandle indexHandle() {
        return indexHandle;
    }

    /** Returns the number of buckets of the Bloom filter. */
    public int filterBucketCount() throws IOException {
        return metadata().filter().bucketCount();
    }

    /** Returns the number of probes per key of the Bloom filter. */
    public int filterProbeCount() throws IOException {
        return metadata().filter().probeCount();
    }

    /** Returns every data block listed in the index, in file order. */
    public List<DataBlockInfo> dataBlocks() throws IOException {
        List<DataBlockInfo> blocks = new ArrayList<>();
        try {
            Block.Iterator it = metadata().index().iterator(InternalKey.COMPARATOR);
            for (it.seekToFirst(); it.valid(); it.next()) {
                blocks.add(new DataBlockInfo(it.key(), decodeHandle(it.value())));
            }
        } catch (DecodeException e) {
            throw damage(indexHandle.offset(), "malformed index block", e);
        }
        return blocks;
    }

    /**
     * Reads and verifies the contents of one block.
     *
     * @throws CorruptionException if the handle points outside the file or the checksum fails
     */
    public byte[] readBlock(BlockHandle handle) throws IOException {
        long offset = handle.offset();
        long size = handle.size();
        if (offset < 0 || size < 0 || size > Integer.MAX_VALUE - 64
                || offset > footerOffset - TableFormat.BLOCK_TRAILER_LENGTH - size) {
            throw damage(offset,
                    "block handle (offset " + offset + ", size " + size + ") is outside the file");
        }
        int contentLength = (int) size;
        byte[] raw = new byte[contentLength + TableFormat.BLOCK_TRAILER_LENGTH];
        readFully(offset, raw, "block");

        CRC32C crc = new CRC32C();
        crc.update(raw, 0, contentLength + 1);
        int expected = Checksums.finishWithContext(crc, fileNumber, offset);
        if (expected != LittleEndian.getInt32(raw, contentLength + 1)) {
            throw damage(offset, "block checksum mismatch");
        }
        if (raw[contentLength] != TableFormat.COMPRESSION_NONE) {
            throw new UnsupportedFormatException("block at " + offset + " of " + name
                    + " uses unknown compression type " + (raw[contentLength] & 0xFF));
        }
        return Arrays.copyOf(raw, contentLength);
    }

    private void readFully(long position, byte[] dst, String what) throws IOException {
        try {
            file.readFully(position, dst, 0, dst.length);
        } catch (EOFException e) {
            throw damage(position, what + " is cut off", e);
        }
    }

    private BlockHandle decodeHandle(byte[] value) {
        if (value.length != BlockHandle.ENCODED_LENGTH) {
            throw new DecodeException("index value of " + value.length + " bytes is not a block handle");
        }
        return BlockHandle.decode(value, 0);
    }

    /**
     * A cursor over all entries of the table in internal-key order. Not safe for concurrent
     * use.
     */
    public final class TableIterator implements InternalIterator {
        private final Block.Iterator indexIterator;
        private Block.Iterator dataIterator;
        private BlockCache.Pinned dataPin; // pin of the current data block, if it is cached
        private long dataBlockOffset = -1;
        private boolean closed;

        private TableIterator(Block index) {
            this.indexIterator = index.iterator(InternalKey.COMPARATOR);
        }

        /** Returns whether the iterator is positioned at an entry. */
        @Override
        public boolean valid() {
            return dataIterator != null && dataIterator.valid();
        }

        /** Moves to the first entry of the table. */
        @Override
        public void seekToFirst() throws IOException {
            requireOpen();
            try {
                indexIterator.seekToFirst();
                loadDataBlock();
                if (dataIterator != null) {
                    dataIterator.seekToFirst();
                }
                skipExhaustedBlocks();
            } catch (DecodeException e) {
                throw malformed(e);
            }
        }

        /** Moves to the first entry whose internal key is at or after {@code internalKey}. */
        @Override
        public void seek(byte[] internalKey) throws IOException {
            requireOpen();
            try {
                indexIterator.seek(internalKey);
                loadDataBlock();
                if (dataIterator != null) {
                    dataIterator.seek(internalKey);
                }
                skipExhaustedBlocks();
            } catch (DecodeException e) {
                throw malformed(e);
            }
        }

        /** Moves to the next entry. */
        @Override
        public void next() throws IOException {
            requireOpen();
            if (!valid()) {
                throw new IllegalStateException("iterator is not positioned at an entry");
            }
            try {
                dataIterator.next();
                skipExhaustedBlocks();
            } catch (DecodeException e) {
                throw malformed(e);
            }
        }

        /** Moves to the last entry of the table. */
        @Override
        public void seekToLast() throws IOException {
            requireOpen();
            try {
                indexIterator.seekToLast();
                loadDataBlock();
                if (dataIterator != null) {
                    dataIterator.seekToLast();
                }
                skipExhaustedBlocksBackward();
            } catch (DecodeException e) {
                throw malformed(e);
            }
        }

        /** Moves to the last entry whose internal key is before {@code internalKey}. */
        @Override
        public void seekBefore(byte[] internalKey) throws IOException {
            requireOpen();
            try {
                // The first block that may hold a key at or after the bound; what is before
                // the bound is in that block or ends the block before it.
                indexIterator.seek(internalKey);
                if (!indexIterator.valid()) {
                    indexIterator.seekToLast();
                    loadDataBlock();
                    if (dataIterator != null) {
                        dataIterator.seekToLast();
                    }
                } else {
                    loadDataBlock();
                    dataIterator.seekBefore(internalKey);
                }
                skipExhaustedBlocksBackward();
            } catch (DecodeException e) {
                throw malformed(e);
            }
        }

        /** Moves to the entry before the current one. */
        @Override
        public void prev() throws IOException {
            requireOpen();
            if (!valid()) {
                throw new IllegalStateException("iterator is not positioned at an entry");
            }
            try {
                dataIterator.prev();
                skipExhaustedBlocksBackward();
            } catch (DecodeException e) {
                throw malformed(e);
            }
        }

        /**
         * Returns the current internal key.
         *
         * @throws CorruptionException if the stored key is not a well-formed internal key
         */
        @Override
        public byte[] key() throws IOException {
            byte[] key = dataIterator().key();
            if (!InternalKey.isWellFormed(key) || InternalKey.userKeyLength(key) == 0) {
                throw damage(dataBlockOffset, "malformed internal key");
            }
            return key;
        }

        /** Returns the current value. */
        @Override
        public byte[] value() {
            return dataIterator().value();
        }

        /** Releases the iterator's reference to the reader. Closing twice has no effect. */
        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                dropDataBlock();
                release();
            }
        }

        private Block.Iterator dataIterator() {
            if (!valid()) {
                throw new IllegalStateException("iterator is not positioned at an entry");
            }
            return dataIterator;
        }

        /** Loads the data block the index iterator points at, or none if it is exhausted. */
        private void loadDataBlock() throws IOException {
            dropDataBlock();
            if (!indexIterator.valid()) {
                return;
            }
            BlockHandle handle = decodeHandle(indexIterator.value());
            dataBlockOffset = handle.offset();
            MemorySegment contents = null;
            if (cache != null) {
                dataPin = cache.get(fileNumber, handle.offset());
                if (dataPin == null) {
                    // Only verified contents enter the cache.
                    byte[] verified = readBlock(handle);
                    dataPin = cache.insert(fileNumber, handle.offset(), verified);
                    if (dataPin == null) {
                        contents = MemorySegment.ofArray(verified);
                    }
                }
                if (dataPin != null) {
                    contents = dataPin.segment();
                }
            } else {
                contents = MemorySegment.ofArray(readBlock(handle));
            }
            dataIterator = new Block(contents).iterator(InternalKey.COMPARATOR);
        }

        /** Forgets the current data block and releases its pin. */
        private void dropDataBlock() {
            dataIterator = null;
            if (dataPin != null) {
                dataPin.close();
                dataPin = null;
            }
        }

        /** While the current data block has no entry left, moves on to the next block. */
        private void skipExhaustedBlocks() throws IOException {
            while (dataIterator != null && !dataIterator.valid()) {
                indexIterator.next();
                loadDataBlock();
                if (dataIterator != null) {
                    dataIterator.seekToFirst();
                }
            }
        }

        /** While the current data block has no entry left, moves back to the block before. */
        private void skipExhaustedBlocksBackward() throws IOException {
            while (dataIterator != null && !dataIterator.valid()) {
                indexIterator.prev();
                loadDataBlock();
                if (dataIterator != null) {
                    dataIterator.seekToLast();
                }
            }
        }

        private CorruptionException malformed(DecodeException e) {
            long offset = dataBlockOffset >= 0 ? dataBlockOffset : indexHandle.offset();
            return damage(offset, "malformed block: " + e.getMessage(), e);
        }

        private void requireOpen() {
            if (closed) {
                throw new IllegalStateException("iterator is closed");
            }
        }
    }
}
