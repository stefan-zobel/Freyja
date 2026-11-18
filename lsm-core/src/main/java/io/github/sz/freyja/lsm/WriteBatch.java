package io.github.sz.freyja.lsm;

import java.util.Arrays;

import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.LittleEndian;

/**
 * A group of puts and deletes that is applied atomically.
 *
 * <p>The batch is also the WAL payload (docs/spec/storage-format.md, section 3.3):
 * {@code StartSequenceNumber} ({@code uint64}) + {@code EntryCount} ({@code uint32}) +
 * entries of {@code [OpType (1 byte), KeyLen (uint32), Key, (ValueLen (uint32), Value if PUT)]}.
 * Entry {@code i} has the sequence number {@code StartSequenceNumber + i}.
 *
 * <p>Not safe for concurrent use.
 */
public final class WriteBatch {

    /** Operation type of a put. */
    public static final byte OP_PUT = 0x01;

    /** Operation type of a delete. */
    public static final byte OP_DELETE = 0x02;

    /** Size of the fields before the first entry. */
    public static final int HEADER_LENGTH = 12;

    /** Receives the entries of a decoded batch in order. */
    public interface Handler {
        void put(long sequence, byte[] key, byte[] value);

        void delete(long sequence, byte[] key);
    }

    private byte[] entries = new byte[64];
    private int size;
    private int count;
    private int largestKey;
    private int largestValue;

    /** Adds a put. The key must not be empty. */
    public WriteBatch put(byte[] key, byte[] value) {
        requireKey(key);
        ensureCapacity(1 + 4 + key.length + 4 + value.length);
        entries[size++] = OP_PUT;
        appendBytes(key);
        appendBytes(value);
        largestValue = Math.max(largestValue, value.length);
        count++;
        return this;
    }

    /** Adds a delete. The key must not be empty. */
    public WriteBatch delete(byte[] key) {
        requireKey(key);
        ensureCapacity(1 + 4 + key.length);
        entries[size++] = OP_DELETE;
        appendBytes(key);
        count++;
        return this;
    }

    /** Returns the number of entries. */
    public int count() {
        return count;
    }

    /** Returns the size of the encoded batch in bytes. */
    public int encodedSize() {
        return HEADER_LENGTH + size;
    }

    /** Returns the length of the longest key. */
    public int largestKeySize() {
        return largestKey;
    }

    /** Returns the length of the longest value. */
    public int largestValueSize() {
        return largestValue;
    }

    /** Removes all entries. */
    public void clear() {
        size = 0;
        count = 0;
        largestKey = 0;
        largestValue = 0;
    }

    /** Returns the WAL payload of this batch with the given first sequence number. */
    public byte[] encode(long startSequence) {
        byte[] out = new byte[HEADER_LENGTH + size];
        LittleEndian.putUint64(out, 0, startSequence);
        LittleEndian.putUint32(out, 8, count);
        System.arraycopy(entries, 0, out, HEADER_LENGTH, size);
        return out;
    }

    /** Passes the entries of this batch to {@code handler}, numbered from {@code startSequence}. */
    public void forEach(long startSequence, Handler handler) {
        decodeEntries(entries, 0, size, startSequence, count, handler);
    }

    /** Returns the first sequence number of an encoded batch. */
    public static long startSequence(byte[] payload) {
        requireHeader(payload);
        return LittleEndian.getUint64(payload, 0);
    }

    /** Returns the number of entries of an encoded batch. */
    public static long entryCount(byte[] payload) {
        requireHeader(payload);
        return LittleEndian.getUint32(payload, 8);
    }

    /**
     * Decodes a WAL payload and passes its entries to {@code handler}.
     *
     * @throws DecodeException if the payload is not a well-formed batch; the handler may have
     *                         received some entries by then
     */
    public static void decode(byte[] payload, Handler handler) {
        requireHeader(payload);
        long start = LittleEndian.getUint64(payload, 0);
        long count = LittleEndian.getUint32(payload, 8);
        if (start < 0 || start + count < start) {
            throw new DecodeException("batch with invalid sequence numbers");
        }
        decodeEntries(payload, HEADER_LENGTH, payload.length, start, count, handler);
    }

    private static void requireHeader(byte[] payload) {
        if (payload.length < HEADER_LENGTH) {
            throw new DecodeException("batch of " + payload.length + " bytes has no header");
        }
    }

    private static void decodeEntries(byte[] buf, int from, int to, long start, long count,
                                      Handler handler) {
        int pos = from;
        for (long i = 0; i < count; i++) {
            if (pos >= to) {
                throw new DecodeException("batch ends before entry " + i + " of " + count);
            }
            byte op = buf[pos++];
            if (op != OP_PUT && op != OP_DELETE) {
                throw new DecodeException("unknown operation type " + op + " in entry " + i);
            }
            int keyLength = readLength(buf, pos, to, i);
            pos += 4;
            if (keyLength == 0) {
                throw new DecodeException("empty key in entry " + i);
            }
            byte[] key = Arrays.copyOfRange(buf, pos, pos + keyLength);
            pos += keyLength;
            if (op == OP_PUT) {
                int valueLength = readLength(buf, pos, to, i);
                pos += 4;
                byte[] value = Arrays.copyOfRange(buf, pos, pos + valueLength);
                pos += valueLength;
                handler.put(start + i, key, value);
            } else {
                handler.delete(start + i, key);
            }
        }
        if (pos != to) {
            throw new DecodeException((to - pos) + " bytes follow the last entry of the batch");
        }
    }

    /** Reads a length field and checks that this many bytes follow it. */
    private static int readLength(byte[] buf, int pos, int to, long entry) {
        if (to - pos < 4) {
            throw new DecodeException("length field of entry " + entry + " is cut off");
        }
        long length = LittleEndian.getUint32(buf, pos);
        if (length > to - pos - 4) {
            throw new DecodeException("entry " + entry + " exceeds the batch");
        }
        return (int) length;
    }

    private void requireKey(byte[] key) {
        if (key.length == 0) {
            throw new IllegalArgumentException("keys must not be empty");
        }
        largestKey = Math.max(largestKey, key.length);
    }

    private void appendBytes(byte[] bytes) {
        LittleEndian.putUint32(entries, size, bytes.length);
        System.arraycopy(bytes, 0, entries, size + 4, bytes.length);
        size += 4 + bytes.length;
    }

    private void ensureCapacity(int extra) {
        long needed = (long) size + extra;
        if (needed > Integer.MAX_VALUE - 64) {
            throw new IllegalArgumentException("batch too large");
        }
        if (needed > entries.length) {
            entries = Arrays.copyOf(entries, (int) Math.max(needed, Math.min(Integer.MAX_VALUE - 64, entries.length * 2L)));
        }
    }
}
