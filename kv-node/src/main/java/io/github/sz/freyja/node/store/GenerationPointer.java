package io.github.sz.freyja.node.store;

import java.io.IOException;
import java.nio.file.Path;

import io.github.sz.freyja.common.LittleEndian;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.raft.SnapshotMeta;

/**
 * The file {@code sm/CURRENT}: which generation of the state machine counts, and what that
 * generation started from (docs/spec/node.md, sections 2 and 5).
 *
 * @param generation     the number of the generation; its directory is named after it
 * @param origin         what the generation held when it became the state machine: nothing
 *                       for the first generation of a node, a snapshot of another node later.
 *                       Its configuration counts until the generation applies a CONFIG entry.
 * @param clusterVersion the cluster version as of the origin; it counts until the generation
 *                       applies a command that raises it
 */
public record GenerationPointer(long generation, SnapshotMeta origin, int clusterVersion) {

    public static final String FILE_NAME = "CURRENT";
    /** The bytes "FRYC" read as a little-endian integer. */
    static final long MAGIC = 0x43595246L;
    static final int VERSION = 1;
    static final long FILE_NUMBER = 5;

    /** The name of the directory of a generation. */
    public static String directoryName(long generation) {
        return String.format("%010d", generation);
    }

    /** Replaces the file. After a crash it names the old or the new generation. */
    public void write(StorageFileSystem fs, Path directory) throws IOException {
        byte[] metaBytes = SnapshotMetaFile.encode(origin);
        byte[] body = new byte[12 + metaBytes.length];
        LittleEndian.putUint64(body, 0, generation);
        LittleEndian.putInt32(body, 8, clusterVersion);
        System.arraycopy(metaBytes, 0, body, 12, metaBytes.length);
        SmallFile.replace(fs, directory.resolve(FILE_NAME), SmallFile.encode(MAGIC, VERSION, body, FILE_NUMBER));
    }

    /** @return what the file says, or {@code null} if it is missing or damaged */
    public static GenerationPointer read(StorageFileSystem fs, Path directory) throws IOException {
        byte[] body = SmallFile.read(fs, directory.resolve(FILE_NAME), MAGIC, VERSION, SmallFile.ANY_LENGTH,
                FILE_NUMBER);
        if (body == null || body.length < 12 + 16) {
            return null;
        }
        try {
            return new GenerationPointer(LittleEndian.getUint64(body, 0), SnapshotMetaFile.decode(body, 12),
                    LittleEndian.getInt32(body, 8));
        } catch (RuntimeException e) {
            return null;
        }
    }
}
