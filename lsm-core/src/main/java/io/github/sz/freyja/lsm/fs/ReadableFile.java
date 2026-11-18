package io.github.sz.freyja.lsm.fs;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/** A handle for positional reads. Safe for concurrent use by several threads. */
public interface ReadableFile extends Closeable {

    /**
     * Reads bytes starting at {@code position} into {@code dst} until {@code dst} is full or
     * the end of the file is reached.
     *
     * @return the number of bytes read; less than {@code dst.remaining()} only at the end of
     *         the file
     */
    int read(long position, ByteBuffer dst) throws IOException;

    /**
     * Reads exactly {@code length} bytes starting at {@code position}.
     *
     * @throws EOFException if the file ends before that
     */
    default void readFully(long position, byte[] dst, int offset, int length) throws IOException {
        int n = read(position, ByteBuffer.wrap(dst, offset, length));
        if (n != length) {
            throw new EOFException("wanted " + length + " bytes at " + position + ", got " + n);
        }
    }

    /** Returns the current size of the file in bytes. */
    long size() throws IOException;

    /** Closes the handle. Closing twice has no effect. */
    @Override
    void close() throws IOException;
}
