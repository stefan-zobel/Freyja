package io.github.sz.freyja.lsm.log;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.CRC32C;

import io.github.sz.freyja.common.Checksums;
import io.github.sz.freyja.common.LittleEndian;
import io.github.sz.freyja.lsm.fs.WritableFile;

/**
 * Appends logical records to a file in the WAL record format
 * (docs/spec/storage-format.md, section 3).
 *
 * <p>The writer emits the header record when it is created. After every {@link #sync()} it
 * remembers the synced length and writes it as a sync point together with the next record.
 * A sync point is never fragmented: if fewer than 15 bytes remain in the current block, it is
 * written at the start of the next block, possibly between two fragments of a record.
 *
 * <p>After any {@link IOException} the state of the file is unknown and the writer must not
 * be used again.
 *
 * <p>Not safe for concurrent use, with one exception: {@link #syncFile} may run on another
 * thread while records are appended.
 */
public final class LogWriter implements Closeable {

    private final WritableFile file;
    private final long fileNumber;
    private long offset;
    private long syncedLength;
    private boolean syncPointPending;

    /** Assembles all fragments of one record so that they reach the file in one append. */
    private byte[] out = new byte[1024];
    private int outLength;

    /**
     * Starts a new log in {@code file}, which must be empty, and writes the header record.
     *
     * @param fileNumber the number of the file; it is bound into every checksum
     */
    public LogWriter(WritableFile file, long fileNumber, FileKind kind) throws IOException {
        if (file.size() != 0) {
            throw new IllegalArgumentException("a log starts in an empty file");
        }
        this.file = file;
        this.fileNumber = fileNumber;
        byte[] header = new byte[LogFormat.FILE_HEADER_PAYLOAD];
        LittleEndian.putUint32(header, 0, LogFormat.MAGIC);
        header[4] = (byte) kind.code();
        LittleEndian.putUint16(header, 5, LogFormat.FORMAT_VERSION);
        addRecord(header, 0, header.length);
    }

    /** Appends one logical record. */
    public void addRecord(byte[] payload) throws IOException {
        addRecord(payload, 0, payload.length);
    }

    /** Appends one logical record of {@code length} bytes starting at {@code start}. */
    public void addRecord(byte[] payload, int start, int length) throws IOException {
        java.util.Objects.checkFromIndexSize(start, length, payload.length);
        outLength = 0;
        long position = offset;
        int pos = start;
        int remaining = length;
        boolean first = true;
        boolean last;
        do {
            int room = roomInBlock(position);
            if (room < LogFormat.HEADER_LENGTH) {
                position += pad(room);
                room = LogFormat.BLOCK_SIZE;
            }
            if (syncPointPending && room >= LogFormat.SYNC_POINT_LENGTH) {
                byte[] synced = new byte[LogFormat.SYNC_POINT_PAYLOAD];
                LittleEndian.putUint64(synced, 0, syncedLength);
                position += fragment(LogFormat.TYPE_SYNC_POINT, synced, 0, synced.length, position);
                syncPointPending = false;
                room = roomInBlock(position);
                if (room < LogFormat.HEADER_LENGTH) {
                    position += pad(room);
                    room = LogFormat.BLOCK_SIZE;
                }
            }
            int n = Math.min(remaining, room - LogFormat.HEADER_LENGTH);
            last = (n == remaining);
            int type = first
                    ? (last ? LogFormat.TYPE_FULL : LogFormat.TYPE_FIRST)
                    : (last ? LogFormat.TYPE_LAST : LogFormat.TYPE_MIDDLE);
            position += fragment(type, payload, pos, n, position);
            pos += n;
            remaining -= n;
            first = false;
        } while (!last);

        file.append(out, 0, outLength);
        offset = position;
    }

    /**
     * Makes everything written so far durable. A failure is final: the caller must not retry
     * and must stop using this log.
     */
    public void sync() throws IOException {
        file.sync();
        synced(offset);
    }

    /**
     * Makes durable at least what was appended before the call, and records nothing: for a
     * sync that runs on another thread while records are still appended. The file must allow
     * that ({@link io.github.sz.freyja.lsm.fs.WritableFile#sync}). When it has returned, the
     * thread that appends calls {@link #synced} with the size it read before the sync began.
     * A failure is final, as for {@link #sync}.
     */
    public void syncFile() throws IOException {
        file.sync();
    }

    /**
     * Records that the first {@code length} bytes are durable: the next record carries that
     * as its sync point.
     */
    public void synced(long length) {
        if (length < 0 || length > offset) {
            throw new IllegalArgumentException("synced length " + length + " of " + offset + " bytes");
        }
        if (length > syncedLength) {
            syncedLength = length;
            syncPointPending = true;
        }
    }

    /** Returns the number of bytes written to the file. */
    public long size() {
        return offset;
    }

    /** Returns the length of the file at the last completed sync. */
    public long syncedLength() {
        return syncedLength;
    }

    /** Closes the file without syncing. */
    @Override
    public void close() throws IOException {
        file.close();
    }

    private static int roomInBlock(long position) {
        return LogFormat.BLOCK_SIZE - (int) (position % LogFormat.BLOCK_SIZE);
    }

    /** Appends {@code count} zero bytes to the output and returns {@code count}. */
    private int pad(int count) {
        ensureCapacity(count);
        Arrays.fill(out, outLength, outLength + count, (byte) 0);
        outLength += count;
        return count;
    }

    /** Appends one fragment to the output and returns its size on disk. */
    private int fragment(int type, byte[] payload, int start, int length, long position) {
        ensureCapacity(LogFormat.HEADER_LENGTH + length);
        int header = outLength;
        LittleEndian.putUint16(out, header + 4, length);
        out[header + 6] = (byte) type;
        System.arraycopy(payload, start, out, header + LogFormat.HEADER_LENGTH, length);
        CRC32C crc = new CRC32C();
        crc.update(out, header + 4, 3 + length);
        LittleEndian.putInt32(out, header, Checksums.finishWithContext(crc, fileNumber, position));
        outLength += LogFormat.HEADER_LENGTH + length;
        return LogFormat.HEADER_LENGTH + length;
    }

    private void ensureCapacity(int extra) {
        if (outLength + extra > out.length) {
            out = Arrays.copyOf(out, Math.max(outLength + extra, out.length * 2));
        }
    }
}
