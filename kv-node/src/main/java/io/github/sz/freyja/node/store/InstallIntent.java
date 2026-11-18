package io.github.sz.freyja.node.store;

import java.io.IOException;
import java.nio.file.Path;

import io.github.sz.freyja.common.LittleEndian;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.raft.SnapshotMeta;

/**
 * The file {@code INSTALL} of a data directory: an installation of a snapshot that has begun
 * (docs/spec/node.md, section 5). The installation changes the state machine, the file
 * {@code SNAPSHOT}, and the log, one after the other. The intent is written first and removed
 * last; a node that finds it at its start completes the installation before anything else.
 *
 * @param meta                what the snapshot covers
 * @param generation          the state-machine generation that holds the received snapshot
 * @param clusterVersion      the cluster version as of the snapshot
 * @param keepSuffix          whether the stored entries after the snapshot stay
 * @param replaceStateMachine whether the generation becomes the state machine
 */
public record InstallIntent(SnapshotMeta meta, long generation, int clusterVersion, boolean keepSuffix,
                            boolean replaceStateMachine) {

    public static final String FILE_NAME = "INSTALL";
    /** The bytes "FRYI" read as a little-endian integer. */
    static final long MAGIC = 0x49595246L;
    static final int VERSION = 1;
    static final long FILE_NUMBER = 4;

    /** Writes the intent. It is durable when the call returns. */
    public void write(StorageFileSystem fs, Path dataDirectory) throws IOException {
        byte[] metaBytes = SnapshotMetaFile.encode(meta);
        byte[] body = new byte[13 + metaBytes.length];
        body[0] = (byte) ((keepSuffix ? 1 : 0) | (replaceStateMachine ? 2 : 0));
        LittleEndian.putUint64(body, 1, generation);
        LittleEndian.putInt32(body, 9, clusterVersion);
        System.arraycopy(metaBytes, 0, body, 13, metaBytes.length);
        SmallFile.replace(fs, dataDirectory.resolve(FILE_NAME),
                SmallFile.encode(MAGIC, VERSION, body, FILE_NUMBER));
    }

    /**
     * @return the intent, or {@code null} if no installation has begun
     * @throws IOException if the file is there but cannot be read: the node cannot know how
     *                     far an installation went
     */
    public static InstallIntent read(StorageFileSystem fs, Path dataDirectory) throws IOException {
        Path path = dataDirectory.resolve(FILE_NAME);
        if (!fs.exists(path)) {
            return null;
        }
        byte[] body = SmallFile.read(fs, path, MAGIC, VERSION, SmallFile.ANY_LENGTH, FILE_NUMBER);
        if (body == null || body.length < 13 + 16 || (body[0] & ~3) != 0) {
            throw new IOException(path + " is damaged; the installation of a snapshot cannot be completed");
        }
        try {
            return new InstallIntent(SnapshotMetaFile.decode(body, 13), LittleEndian.getUint64(body, 1),
                    LittleEndian.getInt32(body, 9), (body[0] & 1) != 0, (body[0] & 2) != 0);
        } catch (RuntimeException e) {
            throw new IOException(path + " is damaged; the installation of a snapshot cannot be completed", e);
        }
    }

    /** Removes the intent: the installation is complete. */
    public static void clear(StorageFileSystem fs, Path dataDirectory) throws IOException {
        Path path = dataDirectory.resolve(FILE_NAME);
        if (fs.exists(path)) {
            fs.delete(path);
            fs.syncDirectory(dataDirectory);
        }
    }
}
