package io.github.sz.freyja.node.store;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import io.github.sz.freyja.common.LittleEndian;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.SnapshotMeta;

/**
 * The file {@code SNAPSHOT} of a data directory: what the stored Raft log starts after
 * (docs/spec/node.md, section 5). It holds the index and the term of the last entry that the
 * log no longer needs to hold, and the configuration as of that entry.
 *
 * <p>The file is replaced before the log gives up entries, so the log always reaches back to
 * the entry after the index in the file.
 */
public final class SnapshotMetaFile {

    public static final String FILE_NAME = "SNAPSHOT";
    /** The bytes "FRYS" read as a little-endian integer. */
    static final long MAGIC = 0x53595246L;
    static final int VERSION = 1;
    static final long FILE_NUMBER = 3;

    private SnapshotMetaFile() {
    }

    /** The encoding of a {@link SnapshotMeta} inside the small files of a node. */
    static byte[] encode(SnapshotMeta meta) {
        byte[] config = meta.config().encode();
        byte[] body = new byte[16 + config.length];
        LittleEndian.putUint64(body, 0, meta.lastIncludedIndex());
        LittleEndian.putUint64(body, 8, meta.lastIncludedTerm());
        System.arraycopy(config, 0, body, 16, config.length);
        return body;
    }

    static SnapshotMeta decode(byte[] body, int offset) {
        return new SnapshotMeta(LittleEndian.getUint64(body, offset), LittleEndian.getUint64(body, offset + 8),
                ClusterConfig.decode(Arrays.copyOfRange(body, offset + 16, body.length)));
    }

    /** Replaces the file. After a crash it holds the old or the new content. */
    public static void write(StorageFileSystem fs, Path dataDirectory, SnapshotMeta meta) throws IOException {
        SmallFile.replace(fs, dataDirectory.resolve(FILE_NAME),
                SmallFile.encode(MAGIC, VERSION, encode(meta), FILE_NUMBER));
    }

    /** @return what the file says, or {@code null} if it is missing or damaged */
    public static SnapshotMeta read(StorageFileSystem fs, Path dataDirectory) throws IOException {
        byte[] body = SmallFile.read(fs, dataDirectory.resolve(FILE_NAME), MAGIC, VERSION, SmallFile.ANY_LENGTH,
                FILE_NUMBER);
        if (body == null || body.length < 16) {
            return null;
        }
        try {
            return decode(body, 0);
        } catch (RuntimeException e) {
            return null;
        }
    }
}
