package io.github.sz.freyja.lsm.manifest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.Varint;
import io.github.sz.freyja.lsm.table.InternalKey;

/**
 * One record of the manifest: a change to the set of live files and to the engine's counters
 * (docs/spec/storage-format.md, section 6).
 *
 * <p>Encoding: a sequence of fields. Each field starts with a tag (varint):
 * <ul>
 *   <li>{@code 1} next file number (varint)</li>
 *   <li>{@code 2} last sequence number (varint)</li>
 *   <li>{@code 3} WAL number below which all WALs are obsolete (varint)</li>
 *   <li>{@code 4} file added: level, file number, file size (varints), smallest internal key,
 *       largest internal key (each a varint length followed by the bytes), smallest and
 *       largest sequence number (varints)</li>
 *   <li>{@code 5} file removed: level, file number (varints)</li>
 * </ul>
 * A field may occur at most once, except for tags 4 and 5. An unknown tag makes the record
 * unreadable for this release.
 */
public final class VersionEdit {

    private static final int TAG_NEXT_FILE_NUMBER = 1;
    private static final int TAG_LAST_SEQUENCE = 2;
    private static final int TAG_MIN_WAL_NUMBER = 3;
    private static final int TAG_ADD_FILE = 4;
    private static final int TAG_REMOVE_FILE = 5;

    /** A file added to a level. */
    public record AddedFile(int level, FileMeta file) {
    }

    /** A file removed from a level. */
    public record RemovedFile(int level, long number) {
    }

    /** Thrown by {@link #decode} for a tag this release does not know. */
    public static final class UnknownTagException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        UnknownTagException(long tag) {
            super("unknown manifest field tag " + tag);
        }
    }

    private Long nextFileNumber;
    private Long lastSequence;
    private Long minWalNumber;
    private final List<AddedFile> addedFiles = new ArrayList<>();
    private final List<RemovedFile> removedFiles = new ArrayList<>();

    public VersionEdit nextFileNumber(long value) {
        this.nextFileNumber = requireNonNegative(value);
        return this;
    }

    public VersionEdit lastSequence(long value) {
        this.lastSequence = requireNonNegative(value);
        return this;
    }

    public VersionEdit minWalNumber(long value) {
        this.minWalNumber = requireNonNegative(value);
        return this;
    }

    public VersionEdit addFile(int level, FileMeta file) {
        addedFiles.add(new AddedFile(level, file));
        return this;
    }

    public VersionEdit removeFile(int level, long number) {
        removedFiles.add(new RemovedFile(level, number));
        return this;
    }

    /** Returns the next file number, or {@code null} if the edit does not set it. */
    public Long nextFileNumber() {
        return nextFileNumber;
    }

    /** Returns the last sequence number, or {@code null} if the edit does not set it. */
    public Long lastSequence() {
        return lastSequence;
    }

    /** Returns the smallest live WAL number, or {@code null} if the edit does not set it. */
    public Long minWalNumber() {
        return minWalNumber;
    }

    public List<AddedFile> addedFiles() {
        return addedFiles;
    }

    public List<RemovedFile> removedFiles() {
        return removedFiles;
    }

    // ---- encoding --------------------------------------------------------------------------

    /** Returns the manifest record for this edit. */
    public byte[] encode() {
        Out out = new Out();
        if (nextFileNumber != null) {
            out.varint(TAG_NEXT_FILE_NUMBER).varint(nextFileNumber);
        }
        if (lastSequence != null) {
            out.varint(TAG_LAST_SEQUENCE).varint(lastSequence);
        }
        if (minWalNumber != null) {
            out.varint(TAG_MIN_WAL_NUMBER).varint(minWalNumber);
        }
        for (RemovedFile removed : removedFiles) {
            out.varint(TAG_REMOVE_FILE).varint(removed.level()).varint(removed.number());
        }
        for (AddedFile added : addedFiles) {
            FileMeta file = added.file();
            out.varint(TAG_ADD_FILE).varint(added.level()).varint(file.number()).varint(file.size())
                    .bytes(file.smallestKey()).bytes(file.largestKey())
                    .varint(file.smallestSequence()).varint(file.largestSequence());
        }
        return out.toArray();
    }

    /**
     * Decodes a manifest record.
     *
     * @throws DecodeException     if the record is malformed
     * @throws UnknownTagException if it contains a field this release does not know
     */
    public static VersionEdit decode(byte[] record) {
        In in = new In(record);
        VersionEdit edit = new VersionEdit();
        while (in.hasMore()) {
            long tag = in.varint();
            if (tag == TAG_NEXT_FILE_NUMBER) {
                requireUnset(edit.nextFileNumber, tag);
                edit.nextFileNumber = in.varint();
            } else if (tag == TAG_LAST_SEQUENCE) {
                requireUnset(edit.lastSequence, tag);
                edit.lastSequence = in.varint();
            } else if (tag == TAG_MIN_WAL_NUMBER) {
                requireUnset(edit.minWalNumber, tag);
                edit.minWalNumber = in.varint();
            } else if (tag == TAG_REMOVE_FILE) {
                edit.removedFiles.add(new RemovedFile(in.level(), in.varint()));
            } else if (tag == TAG_ADD_FILE) {
                int level = in.level();
                long number = in.varint();
                long size = in.varint();
                byte[] smallest = in.internalKey();
                byte[] largest = in.internalKey();
                long smallestSequence = in.varint();
                long largestSequence = in.varint();
                if (InternalKey.compare(smallest, largest) > 0 || smallestSequence > largestSequence) {
                    throw new DecodeException("file " + number + " has an inverted range");
                }
                edit.addedFiles.add(new AddedFile(level,
                        new FileMeta(number, size, smallest, largest, smallestSequence, largestSequence)));
            } else {
                throw new UnknownTagException(tag);
            }
        }
        return edit;
    }

    private static void requireUnset(Long current, long tag) {
        if (current != null) {
            throw new DecodeException("manifest field " + tag + " occurs twice");
        }
    }

    private static long requireNonNegative(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("negative value: " + value);
        }
        return value;
    }

    /** A growing output buffer. */
    private static final class Out {
        private byte[] buf = new byte[128];
        private int size;

        Out varint(long value) {
            ensure(Varint.MAX_LENGTH);
            size = Varint.putUint64(buf, size, value);
            return this;
        }

        Out bytes(byte[] bytes) {
            varint(bytes.length);
            ensure(bytes.length);
            System.arraycopy(bytes, 0, buf, size, bytes.length);
            size += bytes.length;
            return this;
        }

        byte[] toArray() {
            return Arrays.copyOf(buf, size);
        }

        private void ensure(int extra) {
            if (size + extra > buf.length) {
                buf = Arrays.copyOf(buf, Math.max(size + extra, buf.length * 2));
            }
        }
    }

    /** A bounds-checked input cursor. */
    private static final class In {
        private final byte[] buf;
        private int pos;

        In(byte[] buf) {
            this.buf = buf;
        }

        boolean hasMore() {
            return pos < buf.length;
        }

        /** Reads a varint that must fit in a non-negative {@code long}. */
        long varint() {
            long value = Varint.getUint64(buf, pos, buf.length);
            if (value < 0) {
                throw new DecodeException("value exceeds 63 bits at offset " + pos);
            }
            pos += Varint.encodedLength(value);
            return value;
        }

        int level() {
            long level = varint();
            if (level > 255) {
                throw new DecodeException("implausible level " + level);
            }
            return (int) level;
        }

        byte[] internalKey() {
            long length = varint();
            if (length > buf.length - pos) {
                throw new DecodeException("key of " + length + " bytes exceeds the record");
            }
            byte[] key = Arrays.copyOfRange(buf, pos, pos + (int) length);
            pos += (int) length;
            if (!InternalKey.isWellFormed(key) || InternalKey.userKeyLength(key) == 0) {
                throw new DecodeException("malformed internal key in the manifest");
            }
            return key;
        }
    }
}
