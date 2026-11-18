package io.github.stefan_zobel.freyja.node.store;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import io.github.stefan_zobel.freyja.common.LittleEndian;
import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;
import io.github.stefan_zobel.freyja.raft.ClusterConfig;

/**
 * The file {@code BOOTSTRAP} of a data directory: what the node was told when its directory
 * was prepared (docs/spec/node.md, section 2). It is written once.
 *
 * <p>An empty state machine that is to apply the log from its first entry must start from
 * exactly this, on every node. A node needs it again when it rebuilds its state machine.
 *
 * @param config         the configuration the cluster started with; empty for a node that
 *                       joined a cluster
 * @param clusterVersion the cluster version the cluster started with
 */
public record Bootstrap(ClusterConfig config, int clusterVersion) {

    public static final String FILE_NAME = "BOOTSTRAP";
    /** The bytes "FRYB" read as a little-endian integer. */
    static final long MAGIC = 0x42595246L;
    static final int VERSION = 1;
    static final long FILE_NUMBER = 7;

    /** Writes the file. The directory must not have one yet. */
    public void write(StorageFileSystem fs, Path dataDirectory) throws IOException {
        Path path = dataDirectory.resolve(FILE_NAME);
        if (fs.exists(path)) {
            throw new IOException(path + " exists already; it is written once");
        }
        byte[] encoded = config.encode();
        byte[] body = new byte[4 + encoded.length];
        LittleEndian.putInt32(body, 0, clusterVersion);
        System.arraycopy(encoded, 0, body, 4, encoded.length);
        SmallFile.replace(fs, path, SmallFile.encode(MAGIC, VERSION, body, FILE_NUMBER));
    }

    /** @return what the file says, or {@code null} if it is missing or damaged */
    public static Bootstrap read(StorageFileSystem fs, Path dataDirectory) throws IOException {
        byte[] body = SmallFile.read(fs, dataDirectory.resolve(FILE_NAME), MAGIC, VERSION, SmallFile.ANY_LENGTH,
                FILE_NUMBER);
        if (body == null || body.length < 4) {
            return null;
        }
        try {
            return new Bootstrap(ClusterConfig.decode(Arrays.copyOfRange(body, 4, body.length)),
                    LittleEndian.getInt32(body, 0));
        } catch (RuntimeException e) {
            return null;
        }
    }
}
