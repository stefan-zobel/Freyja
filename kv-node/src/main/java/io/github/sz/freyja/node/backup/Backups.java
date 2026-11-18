package io.github.sz.freyja.node.backup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

import io.github.sz.freyja.common.Checksums;
import io.github.sz.freyja.common.LittleEndian;
import io.github.sz.freyja.lsm.CorruptionException;
import io.github.sz.freyja.lsm.UnsupportedFormatException;
import io.github.sz.freyja.lsm.fs.ReadableFile;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.lsm.fs.WritableFile;
import io.github.sz.freyja.node.Node;
import io.github.sz.freyja.node.raftlog.RaftLogStore;
import io.github.sz.freyja.node.sm.StateMachineStore;
import io.github.sz.freyja.node.store.GenerationPointer;
import io.github.sz.freyja.node.store.HardStateStore;
import io.github.sz.freyja.node.store.NodeIdentity;
import io.github.sz.freyja.node.store.SnapshotMetaFile;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.EntryType;
import io.github.sz.freyja.raft.HardState;
import io.github.sz.freyja.raft.LogEntry;
import io.github.sz.freyja.raft.SnapshotMeta;

/**
 * Backups of the state machine and what is made of them (docs/spec/operations.md, section 3).
 *
 * <pre>
 * BACKUP     the manifest: where the backup comes from, what it covers, and every file of it
 *            with its length and its checksum
 * engine/    a checkpoint of the storage engine of the state machine
 * </pre>
 *
 * <p>The manifest is written last. A directory without it is not a backup.
 *
 * <p>The manifest: magic {@code FRYK} ({@code uint32}), format version ({@code uint8}, 1),
 * cluster id ({@code uint64}), node id ({@code uint32}), cluster version ({@code uint32}),
 * last included index and term ({@code uint64} each), length of the encoded configuration
 * ({@code uint32}) and the configuration, number of files ({@code uint32}), and per file the
 * length of its name ({@code uint32}), the name in UTF-8, its length ({@code uint64}), and
 * the CRC32C of its content ({@code uint32}); then the CRC32C of everything before
 * ({@code uint32}). All integers are little-endian.
 */
public final class Backups {

    public static final String MANIFEST = "BACKUP";
    public static final String ENGINE = "engine";
    /** The bytes "FRYK" read as a little-endian integer. */
    private static final int MAGIC = 0x4B595246;
    private static final int VERSION = 1;
    private static final int CHUNK = 1 << 20;

    /** A file of a backup. */
    public record BackupFile(String name, long length, int crc32c) {
    }

    /**
     * What a backup is.
     *
     * @param clusterId      the cluster it was taken from
     * @param nodeId         the node it was taken on
     * @param clusterVersion the cluster version as of the backup
     * @param meta           the last entry the backup includes, and the configuration then
     */
    public record Manifest(long clusterId, int nodeId, int clusterVersion, SnapshotMeta meta, List<BackupFile> files) {
        public Manifest {
            files = List.copyOf(files);
        }
    }

    private Backups() {
    }

    // ------------------------------------------------------------------ taking a backup

    /**
     * Copies a checkpoint of a state machine into a backup directory, file by file, with a
     * checksum of each, and writes the manifest last. The checkpoint may be linked files of a
     * running node: copying reads them and changes nothing.
     *
     * @param checkpoint an engine checkpoint
     * @param target     the backup directory; it must not exist
     */
    public static Manifest write(StorageFileSystem fs, Path checkpoint, Path target, long clusterId, int nodeId,
                                 int clusterVersion, SnapshotMeta meta) throws IOException {
        if (fs.exists(target)) {
            throw new IOException("the backup target exists already: " + target);
        }
        Path engine = target.resolve(ENGINE);
        fs.createDirectories(engine);
        List<BackupFile> files = new ArrayList<>();
        byte[] buffer = new byte[CHUNK];
        for (Path source : fs.list(checkpoint)) {
            String name = source.getFileName().toString();
            CRC32C crc = new CRC32C();
            long length;
            try (ReadableFile in = fs.open(source); WritableFile out = fs.create(engine.resolve(name))) {
                length = in.size();
                for (long offset = 0; offset < length;) {
                    int n = (int) Math.min(CHUNK, length - offset);
                    in.readFully(offset, buffer, 0, n);
                    out.append(buffer, 0, n);
                    crc.update(buffer, 0, n);
                    offset += n;
                }
                out.sync();
            }
            files.add(new BackupFile(name, length, (int) crc.getValue()));
        }
        fs.syncDirectory(engine);
        Manifest manifest = new Manifest(clusterId, nodeId, clusterVersion, meta, files);
        Path temporary = target.resolve(MANIFEST + ".tmp");
        byte[] encoded = encode(manifest);
        try (WritableFile out = fs.create(temporary)) {
            out.append(encoded, 0, encoded.length);
            out.sync();
        }
        fs.rename(temporary, target.resolve(MANIFEST));
        fs.syncDirectory(target);
        return manifest;
    }

    private static byte[] encode(Manifest manifest) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] scratch = new byte[8];
        LittleEndian.putInt32(scratch, 0, MAGIC);
        out.write(scratch, 0, 4);
        out.write(VERSION);
        LittleEndian.putUint64(scratch, 0, manifest.clusterId());
        out.write(scratch, 0, 8);
        LittleEndian.putInt32(scratch, 0, manifest.nodeId());
        out.write(scratch, 0, 4);
        LittleEndian.putInt32(scratch, 0, manifest.clusterVersion());
        out.write(scratch, 0, 4);
        LittleEndian.putUint64(scratch, 0, manifest.meta().lastIncludedIndex());
        out.write(scratch, 0, 8);
        LittleEndian.putUint64(scratch, 0, manifest.meta().lastIncludedTerm());
        out.write(scratch, 0, 8);
        byte[] config = manifest.meta().config().encode();
        LittleEndian.putInt32(scratch, 0, config.length);
        out.write(scratch, 0, 4);
        out.write(config, 0, config.length);
        LittleEndian.putInt32(scratch, 0, manifest.files().size());
        out.write(scratch, 0, 4);
        for (BackupFile file : manifest.files()) {
            byte[] name = file.name().getBytes(StandardCharsets.UTF_8);
            LittleEndian.putInt32(scratch, 0, name.length);
            out.write(scratch, 0, 4);
            out.write(name, 0, name.length);
            LittleEndian.putUint64(scratch, 0, file.length());
            out.write(scratch, 0, 8);
            LittleEndian.putInt32(scratch, 0, file.crc32c());
            out.write(scratch, 0, 4);
        }
        byte[] body = out.toByteArray();
        byte[] all = new byte[body.length + 4];
        System.arraycopy(body, 0, all, 0, body.length);
        LittleEndian.putInt32(all, body.length, Checksums.crc32c(body, 0, body.length));
        return all;
    }

    // ------------------------------------------------------------------ reading a backup

    /**
     * Reads the manifest of a backup.
     *
     * @throws CorruptionException if there is no manifest or it is damaged
     */
    public static Manifest readManifest(StorageFileSystem fs, Path backup) throws IOException {
        Path path = backup.resolve(MANIFEST);
        if (!fs.exists(path)) {
            throw new CorruptionException(MANIFEST, 0, "no manifest: this is not a backup, or it was never completed");
        }
        byte[] bytes;
        try (ReadableFile in = fs.open(path)) {
            long size = in.size();
            if (size < 45 || size > (64 << 20)) {
                throw new CorruptionException(MANIFEST, 0, "a manifest of " + size + " bytes");
            }
            bytes = new byte[(int) size];
            in.readFully(0, bytes, 0, bytes.length);
        }
        int end = bytes.length - 4;
        if (LittleEndian.getInt32(bytes, 0) != MAGIC
                || LittleEndian.getInt32(bytes, end) != Checksums.crc32c(bytes, 0, end)) {
            throw new CorruptionException(MANIFEST, 0, "the manifest is damaged");
        }
        if ((bytes[4] & 0xFF) != VERSION) {
            throw new UnsupportedFormatException("the backup has format version " + (bytes[4] & 0xFF)
                    + "; this software reads version " + VERSION);
        }
        try {
            long clusterId = LittleEndian.getUint64(bytes, 5);
            int nodeId = LittleEndian.getInt32(bytes, 13);
            int clusterVersion = LittleEndian.getInt32(bytes, 17);
            long index = LittleEndian.getUint64(bytes, 21);
            long term = LittleEndian.getUint64(bytes, 29);
            int configLength = LittleEndian.getInt32(bytes, 37);
            int at = 41;
            ClusterConfig config = ClusterConfig.decode(java.util.Arrays.copyOfRange(bytes, at, at + configLength));
            at += configLength;
            int count = LittleEndian.getInt32(bytes, at);
            at += 4;
            List<BackupFile> files = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                int nameLength = LittleEndian.getInt32(bytes, at);
                String name = new String(bytes, at + 4, nameLength, StandardCharsets.UTF_8);
                at += 4 + nameLength;
                files.add(new BackupFile(name, LittleEndian.getUint64(bytes, at), LittleEndian.getInt32(bytes, at + 8)));
                at += 12;
            }
            if (at != end) {
                throw new CorruptionException(MANIFEST, at, "bytes after the end of the manifest");
            }
            return new Manifest(clusterId, nodeId, clusterVersion, new SnapshotMeta(index, term, config), files);
        } catch (RuntimeException e) {
            throw new CorruptionException(MANIFEST, 0, "the manifest is damaged: " + e);
        }
    }

    /**
     * Checks a backup completely: every file of the manifest is there, with its length and
     * its checksum, and nothing else is.
     *
     * @return what is wrong, one line each; empty if the backup is whole
     */
    public static List<String> verify(StorageFileSystem fs, Path backup) throws IOException {
        Manifest manifest = readManifest(fs, backup);
        List<String> wrong = new ArrayList<>();
        Path engine = backup.resolve(ENGINE);
        List<String> present = new ArrayList<>();
        if (fs.exists(engine)) {
            for (Path path : fs.list(engine)) {
                present.add(path.getFileName().toString());
            }
        }
        byte[] buffer = new byte[CHUNK];
        for (BackupFile file : manifest.files()) {
            if (!present.remove(file.name())) {
                wrong.add(file.name() + " is missing");
                continue;
            }
            try (ReadableFile in = fs.open(engine.resolve(file.name()))) {
                long length = in.size();
                if (length != file.length()) {
                    wrong.add(file.name() + " has " + length + " bytes, the manifest says " + file.length());
                    continue;
                }
                CRC32C crc = new CRC32C();
                for (long offset = 0; offset < length;) {
                    int n = (int) Math.min(CHUNK, length - offset);
                    in.readFully(offset, buffer, 0, n);
                    crc.update(buffer, 0, n);
                    offset += n;
                }
                if ((int) crc.getValue() != file.crc32c()) {
                    wrong.add(file.name() + " does not have the checksum of the manifest");
                }
            }
        }
        for (String extra : present) {
            wrong.add(extra + " is not part of the backup");
        }
        return wrong;
    }

    // ------------------------------------------------------------------ making a node of it

    /**
     * Creates the data directory of the first node of a new cluster from a backup: the state
     * machine is the backup, and the log holds one entry, which makes this node the only
     * voter. Further members are added as learners.
     *
     * @param identity the new cluster and the id of the node in it
     * @throws CorruptionException if the backup is not whole
     */
    public static Manifest restore(StorageFileSystem fs, Path backup, Path dataDirectory, NodeIdentity identity)
            throws IOException {
        List<String> wrong = verify(fs, backup);
        if (!wrong.isEmpty()) {
            throw new CorruptionException(MANIFEST, 0, "the backup is not whole: " + wrong);
        }
        Manifest manifest = readManifest(fs, backup);
        if (identity.clusterId() == manifest.clusterId()) {
            throw new IOException("a restored cluster needs a new cluster id: nodes of the old cluster that are"
                    + " still somewhere must not take it for their own");
        }
        ClusterConfig alone = ClusterConfig.ofVoters(identity.nodeId());
        Node.initialize(fs, dataDirectory, identity, alone, manifest.clusterVersion());

        // The state machine: the files of the backup as the next generation.
        Path machines = dataDirectory.resolve(StateMachineStore.DIRECTORY);
        Path generation = machines.resolve(GenerationPointer.directoryName(2));
        fs.createDirectories(generation);
        byte[] buffer = new byte[CHUNK];
        for (BackupFile file : manifest.files()) {
            try (ReadableFile in = fs.open(backup.resolve(ENGINE).resolve(file.name()));
                 WritableFile out = fs.create(generation.resolve(file.name()))) {
                for (long offset = 0; offset < file.length();) {
                    int n = (int) Math.min(CHUNK, file.length() - offset);
                    in.readFully(offset, buffer, 0, n);
                    out.append(buffer, 0, n);
                    offset += n;
                }
                out.sync();
            }
        }
        fs.syncDirectory(generation);
        new GenerationPointer(2, manifest.meta(), manifest.clusterVersion()).write(fs, machines);

        // The log starts after the backup and holds the entry that makes this node the
        // only voter. Applying it tells the state machine too.
        appendMembership(fs, dataDirectory, manifest.meta(), manifest.meta().lastIncludedTerm() + 1, alone);
        return manifest;
    }

    /**
     * Starts a new cluster from the data directory of one node that is not running, after
     * the others are lost for good (docs/spec/operations.md, section 3). The node gets a new
     * cluster id and becomes the only voter.
     *
     * <p><b>This is not safe.</b> Whatever the lost nodes had acknowledged and this node
     * never received is gone, and nobody can say whether there was such a thing.
     *
     * @param newClusterId the id of the cluster that begins here
     * @return the last index of the log of the node before the entry that was added
     */
    public static long recoverFromLostMajority(StorageFileSystem fs, Path dataDirectory, long newClusterId)
            throws IOException {
        NodeIdentity old = NodeIdentity.read(fs, dataDirectory);
        if (old.clusterId() == newClusterId) {
            throw new IOException("the cluster that begins here needs a new cluster id");
        }
        SnapshotMeta snapshot = SnapshotMetaFile.read(fs, dataDirectory);
        if (snapshot == null) {
            throw new IOException(SnapshotMetaFile.FILE_NAME + " is missing or damaged; start the node once, so"
                    + " that it finds where its log starts, and stop it again");
        }
        HardStateStore hardState = HardStateStore.open(fs, dataDirectory);
        HardState stored = hardState.get();
        if (stored.logRecovering()) {
            throw new IOException("this node had lost part of its log and was refilling it; a new cluster cannot"
                    + " begin from it");
        }
        ClusterConfig alone = ClusterConfig.ofVoters(old.nodeId());
        long last;
        try (RaftLogStore log = RaftLogStore.open(fs, dataDirectory.resolve(Node.RAFT_DIRECTORY), 64L << 20,
                snapshot.lastIncludedIndex(), snapshot.config())) {
            if (log.firstDamagedIndex() != -1) {
                throw new IOException("the log of this node is damaged; a new cluster cannot begin from it");
            }
            last = Math.max(log.lastIndex(), snapshot.lastIncludedIndex());
            long lastTerm = log.lastIndex() >= log.firstIndex() ? log.termAt(log.lastIndex())
                    : snapshot.lastIncludedTerm();
            long term = Math.max(stored.term(), lastTerm) + 1;
            log.append(List.of(new LogEntry(last + 1, term, EntryType.CONFIG, alone.encode())));
            log.sync();
            hardState.put(new HardState(term, HardState.NO_VOTE, false, stored.logEpoch()));
        }
        // The identity is written once; the cluster that begins here is another one.
        fs.delete(dataDirectory.resolve(NodeIdentity.FILE_NAME));
        fs.syncDirectory(dataDirectory);
        new NodeIdentity(newClusterId, old.nodeId()).write(fs, dataDirectory);
        return last;
    }

    private static void appendMembership(StorageFileSystem fs, Path dataDirectory, SnapshotMeta meta, long term,
                                         ClusterConfig config) throws IOException {
        SnapshotMetaFile.write(fs, dataDirectory, meta);
        try (RaftLogStore log = RaftLogStore.open(fs, dataDirectory.resolve(Node.RAFT_DIRECTORY), 64L << 20,
                meta.lastIncludedIndex(), meta.config())) {
            log.append(List.of(new LogEntry(meta.lastIncludedIndex() + 1, term, EntryType.CONFIG, config.encode())));
            log.sync();
        }
        HardStateStore.open(fs, dataDirectory).put(new HardState(term, HardState.NO_VOTE, false));
    }
}
