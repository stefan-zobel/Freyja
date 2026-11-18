package io.github.sz.freyja.lsm.log;

/**
 * Constants of the WAL record format (docs/spec/storage-format.md, section 3), which is used
 * by write-ahead logs, the manifest, and Raft log segments.
 */
public final class LogFormat {

    /** The file is a sequence of blocks of this size. */
    public static final int BLOCK_SIZE = 32 * 1024;

    /** Size of a fragment header: checksum (4), length (2), type (1). */
    public static final int HEADER_LENGTH = 7;

    /** Largest payload of one fragment. */
    public static final int MAX_FRAGMENT_PAYLOAD = BLOCK_SIZE - HEADER_LENGTH;

    /** Fragment that holds a complete logical record. */
    public static final int TYPE_FULL = 0x01;

    /** First fragment of a logical record that continues. */
    public static final int TYPE_FIRST = 0x02;

    /** Inner fragment of a logical record. */
    public static final int TYPE_MIDDLE = 0x03;

    /** Last fragment of a logical record. */
    public static final int TYPE_LAST = 0x04;

    /** A sync point; never fragmented. */
    public static final int TYPE_SYNC_POINT = 0x05;

    /** Payload length of a sync point: the synced length as {@code uint64}. */
    public static final int SYNC_POINT_PAYLOAD = 8;

    /** Total size of a sync point on disk. */
    public static final int SYNC_POINT_LENGTH = HEADER_LENGTH + SYNC_POINT_PAYLOAD;

    /** Magic number of the header record; stored as the bytes {@code "FRYJ"}. */
    public static final long MAGIC = 0x4A595246L;

    /** The format version written by this release. */
    public static final int FORMAT_VERSION = 1;

    /** Payload length of the header record: magic (4), file kind (1), version (2). */
    public static final int FILE_HEADER_PAYLOAD = 7;

    private LogFormat() {
    }
}
