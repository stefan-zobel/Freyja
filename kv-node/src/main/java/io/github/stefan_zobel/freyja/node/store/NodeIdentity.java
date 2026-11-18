package io.github.stefan_zobel.freyja.node.store;

import java.io.IOException;
import java.nio.file.Path;

import io.github.stefan_zobel.freyja.common.LittleEndian;
import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;

/**
 * Who a node is: the cluster it belongs to and its id in that cluster. It is written once,
 * when the data directory is initialized, into the file {@code NODE} (docs/spec/node.md,
 * section 2). Node ids are never reused.
 */
public record NodeIdentity(long clusterId, int nodeId) {

    /** The name of the identity file in the data directory. */
    public static final String FILE_NAME = "NODE";

    /** The bytes "FRYN" read as a little-endian integer. */
    static final long MAGIC = 0x4E595246L;
    static final int VERSION = 1;
    /** The number that stands for this file in its checksum. */
    static final long FILE_NUMBER = 0x4E;
    private static final int BODY = 12;

    public NodeIdentity {
        if (nodeId < 0) {
            throw new IllegalArgumentException("node id " + nodeId);
        }
    }

    /**
     * Writes the identity file. The directory must not have one yet.
     *
     * @throws IOException if an identity file exists already
     */
    public void write(StorageFileSystem fs, Path dataDirectory) throws IOException {
        Path path = dataDirectory.resolve(FILE_NAME);
        if (fs.exists(path)) {
            throw new IOException(path + " exists already; an identity is written once");
        }
        byte[] body = new byte[BODY];
        LittleEndian.putUint64(body, 0, clusterId);
        LittleEndian.putInt32(body, 8, nodeId);
        SmallFile.replace(fs, path, SmallFile.encode(MAGIC, VERSION, body, FILE_NUMBER));
    }

    /** Whether the directory has an identity file, readable or not. */
    public static boolean exists(StorageFileSystem fs, Path dataDirectory) throws IOException {
        return fs.exists(dataDirectory.resolve(FILE_NAME));
    }

    /**
     * Reads the identity file.
     *
     * @throws LostIdentityException if the file is missing or damaged
     */
    public static NodeIdentity read(StorageFileSystem fs, Path dataDirectory) throws IOException {
        Path path = dataDirectory.resolve(FILE_NAME);
        byte[] body = SmallFile.read(fs, path, MAGIC, VERSION, BODY, FILE_NUMBER);
        if (body == null) {
            throw new LostIdentityException(path + " is missing or damaged; the node does not know who it is");
        }
        return new NodeIdentity(LittleEndian.getUint64(body, 0), LittleEndian.getInt32(body, 8));
    }
}
