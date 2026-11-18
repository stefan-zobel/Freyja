package io.github.sz.freyja.lsm.table;

import io.github.sz.freyja.common.LittleEndian;

/**
 * The location of a block in a table file.
 *
 * @param offset offset of the first byte of the block contents
 * @param size   length of the block contents, excluding the block trailer
 */
public record BlockHandle(long offset, long size) {

    /** Length of an encoded handle: two little-endian {@code uint64} values. */
    public static final int ENCODED_LENGTH = 16;

    /** Writes this handle at {@code pos}. */
    public void encodeTo(byte[] buf, int pos) {
        LittleEndian.putUint64(buf, pos, offset);
        LittleEndian.putUint64(buf, pos + 8, size);
    }

    /** Returns the encoded handle. */
    public byte[] encode() {
        byte[] out = new byte[ENCODED_LENGTH];
        encodeTo(out, 0);
        return out;
    }

    /** Reads a handle from {@code pos}. The caller validates the values against the file. */
    public static BlockHandle decode(byte[] buf, int pos) {
        return new BlockHandle(LittleEndian.getUint64(buf, pos), LittleEndian.getUint64(buf, pos + 8));
    }
}
