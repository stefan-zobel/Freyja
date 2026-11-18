package io.github.sz.freyja.lsm.table;

/** Constants of the SSTable format (docs/spec/storage-format.md, section 5). */
public final class TableFormat {

    /** The format version written by this release. */
    public static final int FORMAT_VERSION = 1;

    /** Footer magic number; its low 16 bits are the format version. */
    public static final long MAGIC = 0x4C534D4A32350000L | FORMAT_VERSION;

    /** Mask that selects the format version inside the magic number. */
    public static final long VERSION_MASK = 0xFFFFL;

    /** Size of the footer at the end of every table file. */
    public static final int FOOTER_LENGTH = 48;

    /** Number of footer bytes before the footer checksum. */
    public static final int FOOTER_CHECKSUM_OFFSET = 36;

    /** Size of the trailer after every block: compression type and checksum. */
    public static final int BLOCK_TRAILER_LENGTH = 5;

    /** Compression type of an uncompressed block; the only one defined. */
    public static final byte COMPRESSION_NONE = 0x00;

    /** A data block is closed before an entry that would make it larger than this. */
    public static final int BLOCK_SIZE_TARGET = 4096;

    /** Every this many entries a block stores a full key. */
    public static final int RESTART_INTERVAL = 16;

    private TableFormat() {
    }
}
