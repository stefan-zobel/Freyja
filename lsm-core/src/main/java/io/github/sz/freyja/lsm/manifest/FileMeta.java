package io.github.sz.freyja.lsm.manifest;

import java.util.Arrays;

import io.github.sz.freyja.common.ByteComparator;
import io.github.sz.freyja.lsm.table.InternalKey;

/**
 * What the manifest records about one table file.
 *
 * @param number           the file number
 * @param size             the file size in bytes
 * @param smallestKey      the first internal key of the table
 * @param largestKey       the last internal key of the table
 * @param smallestSequence the smallest sequence number of any entry
 * @param largestSequence  the largest sequence number of any entry
 */
public record FileMeta(long number, long size, byte[] smallestKey, byte[] largestKey,
                       long smallestSequence, long largestSequence) {

    /** Returns whether the user-key range of the table includes {@code userKey}. */
    public boolean mayContainUserKey(byte[] userKey) {
        return ByteComparator.compare(userKey, 0, userKey.length,
                        smallestKey, 0, InternalKey.userKeyLength(smallestKey)) >= 0
                && ByteComparator.compare(userKey, 0, userKey.length,
                        largestKey, 0, InternalKey.userKeyLength(largestKey)) <= 0;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof FileMeta that
                && number == that.number && size == that.size
                && smallestSequence == that.smallestSequence && largestSequence == that.largestSequence
                && Arrays.equals(smallestKey, that.smallestKey)
                && Arrays.equals(largestKey, that.largestKey);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(number);
    }
}
