package io.github.sz.freyja.lsm.fs;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An append-only file handle. Not safe for concurrent use by several threads, with one
 * exception: {@link #sync} may run on one thread while another appends.
 */
public interface WritableFile extends Closeable {

    /** Appends all remaining bytes of {@code src} to the end of the file. */
    void append(ByteBuffer src) throws IOException;

    /** Appends {@code length} bytes of {@code buf} starting at {@code offset}. */
    default void append(byte[] buf, int offset, int length) throws IOException {
        append(ByteBuffer.wrap(buf, offset, length));
    }

    /**
     * Makes every byte appended so far durable. A failure leaves the durable state of the
     * file unknown; the caller must not retry.
     *
     * <p>If another thread appends while the sync runs, at least every byte whose append had
     * returned before the sync began is durable when it returns.
     */
    void sync() throws IOException;

    /** Returns the number of bytes in the file, including bytes not yet synced. */
    long size();

    /** Closes the handle without syncing. Closing twice has no effect. */
    @Override
    void close() throws IOException;
}
