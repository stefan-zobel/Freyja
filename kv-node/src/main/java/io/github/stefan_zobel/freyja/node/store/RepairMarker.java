package io.github.stefan_zobel.freyja.node.store;

import java.io.IOException;
import java.nio.file.Path;

import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;

/**
 * The file {@code REPAIR} of a data directory: the state machine was found damaged and must
 * not be used again (docs/spec/node.md, section 6). The node that finds the damage writes
 * the marker and stops; at its next start it rebuilds the state machine and removes the
 * marker when that is done.
 *
 * <p>Only the presence of the file counts. Its content is the one valid record of its kind,
 * so that the file reads like every other small file of a node.
 */
public final class RepairMarker {

    public static final String FILE_NAME = "REPAIR";
    /** The bytes "FRYR" read as a little-endian integer. */
    static final long MAGIC = 0x52595246L;
    static final int VERSION = 1;
    static final long FILE_NUMBER = 6;

    private RepairMarker() {
    }

    /** Sets the marker. It is durable when the call returns. */
    public static void write(StorageFileSystem fs, Path dataDirectory) throws IOException {
        SmallFile.replace(fs, dataDirectory.resolve(FILE_NAME),
                SmallFile.encode(MAGIC, VERSION, new byte[0], FILE_NUMBER));
    }

    public static boolean exists(StorageFileSystem fs, Path dataDirectory) throws IOException {
        return fs.exists(dataDirectory.resolve(FILE_NAME));
    }

    /** Removes the marker: the state machine is rebuilt, or about to be refilled. */
    public static void clear(StorageFileSystem fs, Path dataDirectory) throws IOException {
        Path path = dataDirectory.resolve(FILE_NAME);
        if (fs.exists(path)) {
            fs.delete(path);
            fs.syncDirectory(dataDirectory);
        }
    }
}
