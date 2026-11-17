package io.github.stefan_zobel.freyja.lsm.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32C;

import io.github.stefan_zobel.freyja.common.Checksums;
import io.github.stefan_zobel.freyja.common.LittleEndian;
import io.github.stefan_zobel.freyja.lsm.CorruptionException;
import io.github.stefan_zobel.freyja.lsm.UnsupportedFormatException;
import io.github.stefan_zobel.freyja.lsm.fs.ReadableFile;

/**
 * Replays a file in the WAL record format and classifies its end
 * (docs/spec/storage-format.md, sections 3 and 3.2).
 *
 * <p>The reader delivers every complete logical record up to the first invalid fragment.
 * What follows depends on the file:
 * <ul>
 *   <li>A file without an invalid fragment is clean.</li>
 *   <li>In a sealed file an invalid fragment is damage.</li>
 *   <li>In the last file of its kind it is damage if a valid sync point, before or after the
 *       invalid fragment, proves that bytes beyond the last complete record had been synced.
 *       Otherwise it is a torn tail, and the file may be cut at {@link Result#validLength}.</li>
 * </ul>
 * Damage is reported as {@link CorruptionException}. The reader never changes the file; the
 * owner truncates a torn tail.
 */
public final class LogReader {

    /** Receives the content of a log during {@link #read}. */
    public interface Listener {

        /**
         * Called for every complete logical record after the header record.
         *
         * @param offset  offset of the first fragment of the record
         * @param payload the record
         */
        void record(long offset, byte[] payload) throws IOException;

        /**
         * Called for every valid sync point.
         *
         * @param offset       offset of the sync point
         * @param syncedLength the synced length it records
         */
        default void syncPoint(long offset, long syncedLength) throws IOException {
        }
    }

    /**
     * Outcome of a replay that found no damage.
     *
     * @param kind          the file kind from the header record; {@code null} for an empty file
     * @param validLength   the length to keep: the file size if the file is clean, otherwise
     *                      the end of the last complete logical record
     * @param tornTail      whether the file ends in a torn tail
     * @param failureOffset offset of the first invalid fragment, or -1 if the file is clean
     * @param failure       what was wrong at {@code failureOffset}, or {@code null}
     */
    public record Result(FileKind kind, long validLength, boolean tornTail,
                         long failureOffset, String failure) {
    }

    private LogReader() {
    }

    /**
     * Replays {@code file}.
     *
     * @param name         name of the file for error messages
     * @param fileNumber   the number under which the file was written
     * @param expectedKind the kind the file must have, or {@code null} to accept any
     * @param sealed       whether a successor of the same kind exists; a sealed file was
     *                     synced completely, so it cannot have a torn tail
     * @throws CorruptionException        on damage, or if the file has another kind
     * @throws UnsupportedFormatException if the file uses an unknown format version
     */
    public static Result read(ReadableFile file, String name, long fileNumber,
                              FileKind expectedKind, boolean sealed, Listener listener)
            throws IOException {
        long size = file.size();
        byte[] block = new byte[LogFormat.BLOCK_SIZE];
        byte[] record = new byte[256];
        int recordLength = 0;
        long recordOffset = 0;
        boolean inRecord = false;
        boolean headerSeen = false;
        FileKind kind = null;
        long lastGood = 0;   // end of the last complete logical record or stand-alone sync point
        long maxSynced = 0;  // largest synced length of any sync point read
        long failureOffset = -1;
        String failure = null;

        scan:
        for (long blockStart = 0; blockStart < size; blockStart += LogFormat.BLOCK_SIZE) {
            int n = file.read(blockStart, ByteBuffer.wrap(block));
            if (n != Math.min(LogFormat.BLOCK_SIZE, size - blockStart)) {
                throw new IOException("short read at " + blockStart + " of " + name);
            }
            int p = 0;
            while (p < n) {
                long offset = blockStart + p;
                int room = LogFormat.BLOCK_SIZE - p;
                if (room < LogFormat.HEADER_LENGTH) {
                    for (int i = p; i < n; i++) {
                        if (block[i] != 0) {
                            failureOffset = blockStart + i;
                            failure = "block padding is not zero";
                            break scan;
                        }
                    }
                    if (!inRecord) {
                        lastGood = blockStart + n;
                    }
                    break;
                }
                if (n - p < LogFormat.HEADER_LENGTH) {
                    failureOffset = offset;
                    failure = "fragment header is cut off";
                    break scan;
                }
                int length = LittleEndian.getUint16(block, p + 4);
                int type = block[p + 6] & 0xFF;
                if (type < LogFormat.TYPE_FULL || type > LogFormat.TYPE_SYNC_POINT) {
                    failureOffset = offset;
                    failure = "invalid fragment type " + type;
                    break scan;
                }
                if (length > room - LogFormat.HEADER_LENGTH) {
                    failureOffset = offset;
                    failure = "fragment length " + length + " crosses the block boundary";
                    break scan;
                }
                if (length > n - p - LogFormat.HEADER_LENGTH) {
                    failureOffset = offset;
                    failure = "fragment is cut off";
                    break scan;
                }
                if (!checksumMatches(block, p, length, fileNumber, offset)) {
                    failureOffset = offset;
                    failure = "fragment checksum mismatch";
                    break scan;
                }
                int payload = p + LogFormat.HEADER_LENGTH;
                int end = payload + length;

                if (type == LogFormat.TYPE_SYNC_POINT) {
                    long synced = (length == LogFormat.SYNC_POINT_PAYLOAD)
                            ? LittleEndian.getUint64(block, payload) : -1;
                    if (synced < 0 || synced > offset || !headerSeen) {
                        failureOffset = offset;
                        failure = "sync point is inconsistent";
                        break scan;
                    }
                    maxSynced = Math.max(maxSynced, synced);
                    listener.syncPoint(offset, synced);
                    if (!inRecord) {
                        lastGood = blockStart + end;
                    }
                    p = end;
                    continue;
                }

                boolean starts = (type == LogFormat.TYPE_FULL || type == LogFormat.TYPE_FIRST);
                if (starts == inRecord) {
                    failureOffset = offset;
                    failure = inRecord ? "a record starts inside another record"
                            : "a record continues without a start";
                    break scan;
                }
                if (starts) {
                    recordLength = 0;
                    recordOffset = offset;
                    inRecord = true;
                }
                if (recordLength + length > record.length) {
                    record = Arrays.copyOf(record, Math.max(recordLength + length, record.length * 2));
                }
                System.arraycopy(block, payload, record, recordLength, length);
                recordLength += length;

                if (type == LogFormat.TYPE_FULL || type == LogFormat.TYPE_LAST) {
                    inRecord = false;
                    byte[] complete = Arrays.copyOf(record, recordLength);
                    if (!headerSeen) {
                        kind = parseHeader(complete, name, expectedKind);
                        headerSeen = true;
                    } else {
                        listener.record(recordOffset, complete);
                    }
                    lastGood = blockStart + end;
                }
                p = end;
            }
        }
        if (failure == null && inRecord) {
            failureOffset = size;
            failure = "the last record is incomplete";
        }
        if (failure == null) {
            return new Result(kind, size, false, -1, null);
        }

        // An invalid fragment: damage or a torn tail?
        if (sealed) {
            throw new CorruptionException(name, failureOffset, failure + " in a sealed file");
        }
        if (maxSynced > lastGood) {
            throw new CorruptionException(name, failureOffset, failure
                    + "; an earlier sync point records " + maxSynced + " synced bytes");
        }
        long proof = findSyncPointBeyond(file, size, fileNumber, failureOffset, lastGood);
        if (proof >= 0) {
            throw new CorruptionException(name, failureOffset, failure
                    + "; the sync point at " + proof + " proves that this range was synced");
        }
        return new Result(kind, lastGood, true, failureOffset, failure);
    }

    /** Checks the header record: magic number, file kind, format version. */
    private static FileKind parseHeader(byte[] payload, String name, FileKind expectedKind)
            throws IOException {
        if (payload.length != LogFormat.FILE_HEADER_PAYLOAD
                || LittleEndian.getUint32(payload, 0) != LogFormat.MAGIC) {
            throw new CorruptionException(name, 0, "the first record is not a log header");
        }
        int version = LittleEndian.getUint16(payload, 5);
        if (version != LogFormat.FORMAT_VERSION) {
            throw new UnsupportedFormatException("log " + name + " has format version " + version
                    + "; this release reads version " + LogFormat.FORMAT_VERSION);
        }
        FileKind kind = FileKind.fromCode(payload[4] & 0xFF);
        if (kind == null) {
            throw new UnsupportedFormatException(
                    "log " + name + " has the unknown file kind " + (payload[4] & 0xFF));
        }
        if (expectedKind != null && kind != expectedKind) {
            throw new CorruptionException(name, 0,
                    "expected a " + expectedKind + " file but found a " + kind + " file");
        }
        return kind;
    }

    private static boolean checksumMatches(byte[] buf, int header, int length,
                                           long fileNumber, long offset) {
        CRC32C crc = new CRC32C();
        crc.update(buf, header + 4, 3 + length);
        return Checksums.finishWithContext(crc, fileNumber, offset) == LittleEndian.getInt32(buf, header);
    }

    /**
     * Searches the file from {@code from} on, at every byte offset, for a valid sync point
     * whose synced length exceeds {@code keep}.
     *
     * @return the offset of such a sync point, or -1 if there is none
     */
    private static long findSyncPointBeyond(ReadableFile file, long size, long fileNumber,
                                            long from, long keep) throws IOException {
        int overlap = LogFormat.SYNC_POINT_LENGTH - 1;
        byte[] window = new byte[2 * LogFormat.BLOCK_SIZE];
        long windowStart = from;
        while (size - windowStart >= LogFormat.SYNC_POINT_LENGTH) {
            int n = file.read(windowStart, ByteBuffer.wrap(window));
            for (int p = 0; p + LogFormat.SYNC_POINT_LENGTH <= n; p++) {
                if (window[p + 6] != LogFormat.TYPE_SYNC_POINT
                        || window[p + 4] != LogFormat.SYNC_POINT_PAYLOAD || window[p + 5] != 0) {
                    continue;
                }
                long offset = windowStart + p;
                if (offset % LogFormat.BLOCK_SIZE > LogFormat.BLOCK_SIZE - LogFormat.SYNC_POINT_LENGTH) {
                    continue; // a sync point never crosses a block boundary
                }
                if (!checksumMatches(window, p, LogFormat.SYNC_POINT_PAYLOAD, fileNumber, offset)) {
                    continue;
                }
                long synced = LittleEndian.getUint64(window, p + LogFormat.HEADER_LENGTH);
                if (synced > keep && synced <= offset) {
                    return offset;
                }
            }
            if (n < window.length) {
                break;
            }
            windowStart += n - overlap;
        }
        return -1;
    }
}
