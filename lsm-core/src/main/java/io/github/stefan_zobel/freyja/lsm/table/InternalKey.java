package io.github.stefan_zobel.freyja.lsm.table;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Internal keys: a user key followed by a 9-byte trailer
 * (docs/spec/storage-format.md, section 4).
 *
 * <p>The trailer is the bitwise complement of the sequence number as a <em>big-endian</em>
 * {@code uint64}, followed by the value type. This is the one place in the system that is
 * big-endian on purpose: it makes a plain unsigned byte comparison of two trailers sort the
 * higher sequence number first.
 *
 * <p>Order: user keys as unsigned bytes; for equal user keys, trailers as unsigned bytes.
 * The concatenated bytes must never be compared directly, because user keys have variable
 * length.
 */
public final class InternalKey {

    /** Length of the trailer. */
    public static final int TRAILER_LENGTH = 9;

    /** Value type of a deletion marker. */
    public static final byte TYPE_TOMBSTONE = 0x00;

    /** Value type of a regular value. */
    public static final byte TYPE_VALUE = 0x01;

    /** Largest valid sequence number. Sequence numbers are non-negative {@code long} values. */
    public static final long MAX_SEQUENCE = Long.MAX_VALUE;

    /** The internal-key order. */
    public static final Comparator<byte[]> COMPARATOR = InternalKey::compare;

    private static final VarHandle BIG_ENDIAN_INT64 =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    private InternalKey() {
    }

    /** Builds the internal key for a user key, a sequence number, and a value type. */
    public static byte[] encode(byte[] userKey, long sequence, byte type) {
        if (sequence < 0) {
            throw new IllegalArgumentException("negative sequence number: " + sequence);
        }
        if (type != TYPE_VALUE && type != TYPE_TOMBSTONE) {
            throw new IllegalArgumentException("unknown value type: " + type);
        }
        byte[] key = Arrays.copyOf(userKey, userKey.length + TRAILER_LENGTH);
        BIG_ENDIAN_INT64.set(key, userKey.length, ~sequence);
        key[key.length - 1] = type;
        return key;
    }

    /**
     * Returns the key to seek to for reading {@code userKey} at snapshot {@code sequence}:
     * the smallest internal key of that user key that is visible at the snapshot.
     */
    public static byte[] seekKey(byte[] userKey, long sequence) {
        return encode(userKey, sequence, TYPE_TOMBSTONE);
    }

    /** Returns whether {@code key} has a trailer with a valid sequence number and value type. */
    public static boolean isWellFormed(byte[] key) {
        if (key.length < TRAILER_LENGTH) {
            return false;
        }
        byte type = key[key.length - 1];
        return (type == TYPE_VALUE || type == TYPE_TOMBSTONE) && sequence(key) >= 0;
    }

    /** Returns the length of the user key inside {@code internalKey}. */
    public static int userKeyLength(byte[] internalKey) {
        return internalKey.length - TRAILER_LENGTH;
    }

    /** Returns a copy of the user key. */
    public static byte[] userKey(byte[] internalKey) {
        return Arrays.copyOf(internalKey, userKeyLength(internalKey));
    }

    /** Returns the sequence number. */
    public static long sequence(byte[] internalKey) {
        return ~(long) BIG_ENDIAN_INT64.get(internalKey, userKeyLength(internalKey));
    }

    /** Returns the value type. */
    public static byte type(byte[] internalKey) {
        return internalKey[internalKey.length - 1];
    }

    /** Returns whether the user key inside {@code internalKey} equals {@code userKey}. */
    public static boolean hasUserKey(byte[] internalKey, byte[] userKey) {
        return Arrays.equals(internalKey, 0, userKeyLength(internalKey), userKey, 0, userKey.length);
    }

    /**
     * Compares two internal keys: user key ascending, then sequence number descending, then
     * value type ascending.
     */
    public static int compare(byte[] a, byte[] b) {
        int aUser = a.length - TRAILER_LENGTH;
        int bUser = b.length - TRAILER_LENGTH;
        if (aUser < 0 || bUser < 0) {
            throw new IllegalArgumentException("not an internal key");
        }
        int c = Arrays.compareUnsigned(a, 0, aUser, b, 0, bUser);
        if (c != 0) {
            return c;
        }
        return Arrays.compareUnsigned(a, aUser, a.length, b, bUser, b.length);
    }
}
