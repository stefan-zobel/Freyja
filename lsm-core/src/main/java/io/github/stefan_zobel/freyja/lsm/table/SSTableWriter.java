package io.github.stefan_zobel.freyja.lsm.table;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.zip.CRC32C;

import io.github.stefan_zobel.freyja.common.Checksums;
import io.github.stefan_zobel.freyja.common.KeyHash;
import io.github.stefan_zobel.freyja.common.LittleEndian;
import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;
import io.github.stefan_zobel.freyja.lsm.fs.WritableFile;

/**
 * Writes one table file sequentially (docs/spec/storage-format.md, section 5).
 *
 * <p>Usage: {@link #add} every entry in ascending internal-key order, then {@link #finish()}.
 * {@code finish} syncs and closes the file; the caller makes the file name durable by
 * syncing the directory. A writer that is closed without {@code finish} leaves an incomplete
 * file behind, which the caller deletes.
 *
 * <p>Not safe for concurrent use.
 */
public final class SSTableWriter implements Closeable {

    /**
     * Summary of a finished table.
     *
     * @param fileNumber       the number of the file
     * @param fileSize         the size of the file in bytes
     * @param entryCount       the number of entries
     * @param smallestKey      the first internal key
     * @param largestKey       the last internal key
     * @param smallestSequence the smallest sequence number of any entry
     * @param largestSequence  the largest sequence number of any entry
     */
    public record TableInfo(long fileNumber, long fileSize, long entryCount,
                            byte[] smallestKey, byte[] largestKey,
                            long smallestSequence, long largestSequence) {
    }

    private final WritableFile file;
    private final long fileNumber;
    private final BlockBuilder dataBlock = new BlockBuilder(TableFormat.RESTART_INTERVAL);
    private final BlockBuilder indexBlock = new BlockBuilder(TableFormat.RESTART_INTERVAL);
    private long offset;
    private long[] keyHashes = new long[1024];
    private int keyHashCount;
    private long entryCount;
    private byte[] smallestKey;
    private byte[] lastKey;
    private long smallestSequence = Long.MAX_VALUE;
    private long largestSequence = -1;
    private boolean done;

    /**
     * Creates the file {@code path}, which must not exist.
     *
     * @param fileNumber the number of the file; it is bound into every checksum
     */
    public SSTableWriter(StorageFileSystem fs, Path path, long fileNumber) throws IOException {
        this.fileNumber = fileNumber;
        this.file = fs.create(path);
    }

    /**
     * Appends an entry.
     *
     * @param internalKey a well-formed internal key, greater than every key added before
     */
    public void add(byte[] internalKey, byte[] value) throws IOException {
        if (done) {
            throw new IllegalStateException("writer is finished or closed");
        }
        if (!InternalKey.isWellFormed(internalKey) || InternalKey.userKeyLength(internalKey) == 0) {
            throw new IllegalArgumentException("not a well-formed internal key");
        }
        if (lastKey != null && InternalKey.compare(lastKey, internalKey) >= 0) {
            throw new IllegalArgumentException("keys must be added in strictly ascending order");
        }

        // A block is closed before an entry that would push it past the target size, so
        // only a block with a single large entry exceeds the target.
        if (!dataBlock.isEmpty()
                && dataBlock.sizeWith(internalKey, value) > TableFormat.BLOCK_SIZE_TARGET) {
            flushDataBlock();
        }

        // Versions of one user key are adjacent, so comparing with the previous key is
        // enough to hash every user key once.
        if (lastKey == null || !sameUserKey(lastKey, internalKey)) {
            if (keyHashCount == keyHashes.length) {
                keyHashes = Arrays.copyOf(keyHashes, keyHashCount * 2);
            }
            keyHashes[keyHashCount++] =
                    KeyHash.hash(internalKey, 0, InternalKey.userKeyLength(internalKey));
        }

        if (smallestKey == null) {
            smallestKey = internalKey.clone();
        }
        lastKey = internalKey.clone();
        long sequence = InternalKey.sequence(internalKey);
        smallestSequence = Math.min(smallestSequence, sequence);
        largestSequence = Math.max(largestSequence, sequence);
        entryCount++;

        dataBlock.add(internalKey, value);
    }

    /** Returns the number of entries added so far. */
    public long entryCount() {
        return entryCount;
    }

    /** Returns the number of bytes written to the file so far, excluding the open data block. */
    public long fileSize() {
        return offset;
    }

    /**
     * Writes the filter block, the index block, and the footer, then syncs and closes the
     * file.
     *
     * @throws IllegalStateException if no entry was added; a table is never empty
     */
    public TableInfo finish() throws IOException {
        if (done) {
            throw new IllegalStateException("writer is finished or closed");
        }
        if (entryCount == 0) {
            throw new IllegalStateException("a table must contain at least one entry");
        }
        flushDataBlock();
        BlockHandle filterHandle = writeBlock(BloomFilter.build(keyHashes, keyHashCount));
        BlockHandle indexHandle = writeBlock(indexBlock.finish());

        byte[] footer = new byte[TableFormat.FOOTER_LENGTH];
        filterHandle.encodeTo(footer, 0);
        indexHandle.encodeTo(footer, 16);
        // Bytes 32..35 are padding and stay zero.
        LittleEndian.putUint64(footer, 40, TableFormat.MAGIC);
        CRC32C crc = new CRC32C();
        crc.update(footer, 0, TableFormat.FOOTER_CHECKSUM_OFFSET);
        crc.update(footer, 40, 8);
        LittleEndian.putInt32(footer, TableFormat.FOOTER_CHECKSUM_OFFSET,
                Checksums.finishWithContext(crc, fileNumber, offset));
        file.append(footer, 0, footer.length);
        offset += footer.length;

        file.sync();
        done = true;
        file.close();
        return new TableInfo(fileNumber, offset, entryCount, smallestKey, lastKey,
                smallestSequence, largestSequence);
    }

    /** Closes the file handle. After {@link #finish()} this does nothing. */
    @Override
    public void close() throws IOException {
        done = true;
        file.close();
    }

    private static boolean sameUserKey(byte[] a, byte[] b) {
        int aUser = InternalKey.userKeyLength(a);
        int bUser = InternalKey.userKeyLength(b);
        return Arrays.equals(a, 0, aUser, b, 0, bUser);
    }

    private void flushDataBlock() throws IOException {
        if (dataBlock.isEmpty()) {
            return;
        }
        BlockHandle handle = writeBlock(dataBlock.finish());
        dataBlock.reset();
        indexBlock.add(lastKey, handle.encode());
    }

    /** Writes block contents followed by the block trailer and returns the block's handle. */
    private BlockHandle writeBlock(byte[] contents) throws IOException {
        byte[] trailer = new byte[TableFormat.BLOCK_TRAILER_LENGTH];
        trailer[0] = TableFormat.COMPRESSION_NONE;
        CRC32C crc = new CRC32C();
        crc.update(contents, 0, contents.length);
        crc.update(trailer, 0, 1);
        LittleEndian.putInt32(trailer, 1, Checksums.finishWithContext(crc, fileNumber, offset));

        BlockHandle handle = new BlockHandle(offset, contents.length);
        file.append(contents, 0, contents.length);
        file.append(trailer, 0, trailer.length);
        offset += contents.length + trailer.length;
        return handle;
    }
}
