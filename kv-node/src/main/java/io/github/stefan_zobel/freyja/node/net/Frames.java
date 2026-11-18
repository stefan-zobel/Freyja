package io.github.stefan_zobel.freyja.node.net;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import io.github.stefan_zobel.freyja.common.LittleEndian;

/**
 * Length-prefixed frames on a stream (docs/spec/node.md, section 8): the length of the frame
 * as a little-endian {@code uint32}, then its bytes. What the bytes mean is up to the
 * protocol of the connection; every message carries its own checksum.
 */
public final class Frames {

    private Frames() {
    }

    /** Writes one frame. The caller flushes. */
    public static void write(OutputStream out, byte[] frame) throws IOException {
        byte[] length = new byte[4];
        LittleEndian.putInt32(length, 0, frame.length);
        out.write(length);
        out.write(frame);
    }

    /**
     * Reads one frame.
     *
     * @param maxBytes the largest frame that is accepted
     * @return the frame, or {@code null} if the stream ended before a frame began
     * @throws IOException if the stream ends inside a frame, or a frame is longer than allowed:
     *                     the connection is of no use any more
     */
    public static byte[] read(InputStream in, int maxBytes) throws IOException {
        byte[] length = new byte[4];
        int first = in.read(length, 0, 4);
        if (first == -1) {
            return null;
        }
        readFully(in, length, first, 4 - first);
        int size = LittleEndian.getInt32(length, 0);
        if (size < 0 || size > maxBytes) {
            throw new IOException("a frame of " + (size & 0xFFFFFFFFL) + " bytes; the limit is " + maxBytes);
        }
        byte[] frame = new byte[size];
        readFully(in, frame, 0, size);
        return frame;
    }

    private static void readFully(InputStream in, byte[] buffer, int offset, int length) throws IOException {
        int done = 0;
        while (done < length) {
            int n = in.read(buffer, offset + done, length - done);
            if (n == -1) {
                throw new EOFException("the stream ended inside a frame");
            }
            done += n;
        }
    }
}
