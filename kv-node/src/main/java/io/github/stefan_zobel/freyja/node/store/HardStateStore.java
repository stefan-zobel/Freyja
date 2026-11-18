package io.github.stefan_zobel.freyja.node.store;

import java.io.IOException;
import java.nio.file.Path;

import io.github.stefan_zobel.freyja.common.LittleEndian;
import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;
import io.github.stefan_zobel.freyja.raft.HardState;

/**
 * The durable hard state of a node (docs/spec/node.md, section 3.2): term, vote, and the
 * state of the log-recovering mode, in the two files {@code HARDSTATE-A} and
 * {@code HARDSTATE-B}. Each holds one checksummed record with a version counter.
 *
 * <p>An update replaces A and then B. The state counts as stored when {@link #put} returns:
 * both files then hold it. After a crash in between, {@link #open} finds either the old or
 * the new state, and never a state the node has not at least begun to store.
 *
 * <p>A failed update is fatal for the instance: it refuses further updates. The node must
 * stop; a restart decides from what is on disk.
 *
 * <p>An instance is confined to one thread.
 */
public final class HardStateStore {

    public static final String FILE_A = "HARDSTATE-A";
    public static final String FILE_B = "HARDSTATE-B";

    /** The bytes "FRYH" read as a little-endian integer. */
    static final long MAGIC = 0x48595246L;
    static final int VERSION = 1;
    private static final long NUMBER_A = 1;
    private static final long NUMBER_B = 2;
    /** counter, term, vote, flags, log epoch, lost log index, lost log term */
    private static final int BODY = 8 + 8 + 4 + 1 + 8 + 8 + 8;
    private static final int FLAG_LOG_RECOVERING = 1;

    private final StorageFileSystem fs;
    private final Path directory;
    private HardState state;
    private long counter;
    private boolean failed;

    private record Stored(long counter, HardState state) {
    }

    private HardStateStore(StorageFileSystem fs, Path directory, Stored stored) {
        this.fs = fs;
        this.directory = directory;
        this.state = stored.state();
        this.counter = stored.counter();
    }

    /**
     * Writes the hard state of a node that has never run. The directory must not have
     * hard-state files yet.
     */
    public static HardStateStore initialize(StorageFileSystem fs, Path dataDirectory) throws IOException {
        if (fs.exists(dataDirectory.resolve(FILE_A)) || fs.exists(dataDirectory.resolve(FILE_B))) {
            throw new IOException(dataDirectory + " has hard-state files already");
        }
        HardStateStore store = new HardStateStore(fs, dataDirectory, new Stored(0, HardState.INITIAL));
        store.put(HardState.INITIAL);
        return store;
    }

    /**
     * Reads the stored hard state.
     *
     * <ul>
     *   <li>Both files valid: the one with the higher counter counts.</li>
     *   <li>One valid: it counts. If A is unreadable, its update never completed or it was
     *       damaged later, and B holds a state the node had fully stored. If B is unreadable,
     *       A is at least as new.</li>
     * </ul>
     *
     * @throws LostIdentityException if neither file holds a valid record
     */
    public static HardStateStore open(StorageFileSystem fs, Path dataDirectory) throws IOException {
        Path a = dataDirectory.resolve(FILE_A);
        Path b = dataDirectory.resolve(FILE_B);
        Stored inA = read(fs, a, NUMBER_A);
        Stored inB = read(fs, b, NUMBER_B);
        if (inA == null && inB == null) {
            throw new LostIdentityException("neither " + a + " nor " + b
                    + " holds a valid record; the node does not know what it has promised");
        }
        // A temporary file is what a crash left of an update that did not complete.
        for (Path file : new Path[] {a, b}) {
            Path temporary = SmallFile.temporaryOf(file);
            if (fs.exists(temporary)) {
                fs.delete(temporary);
            }
        }
        Stored newest = inB == null || (inA != null && inA.counter() >= inB.counter()) ? inA : inB;
        HardStateStore store = new HardStateStore(fs, dataDirectory, newest);
        if (inA == null || inB == null || inA.counter() != inB.counter()) {
            // Make both files hold the state again before the node acts on it.
            store.put(newest.state());
        }
        return store;
    }

    /** The hard state that is stored. */
    public HardState get() {
        return state;
    }

    /** The version counter of the stored state. */
    public long counter() {
        return counter;
    }

    /**
     * Stores {@code next}: replaces A, then B. When this returns, both hold it.
     *
     * @throws IOException if the update failed; the instance refuses further updates then
     */
    public void put(HardState next) throws IOException {
        if (failed) {
            throw new IOException("an earlier update of the hard state failed; the node must stop");
        }
        long nextCounter = counter + 1;
        failed = true;
        SmallFile.replace(fs, directory.resolve(FILE_A), encode(nextCounter, next, NUMBER_A));
        SmallFile.replace(fs, directory.resolve(FILE_B), encode(nextCounter, next, NUMBER_B));
        failed = false;
        counter = nextCounter;
        state = next;
    }

    private static byte[] encode(long counter, HardState state, long fileNumber) {
        byte[] body = new byte[BODY];
        LittleEndian.putUint64(body, 0, counter);
        LittleEndian.putUint64(body, 8, state.term());
        LittleEndian.putInt32(body, 16, state.votedFor());
        body[20] = (byte) (state.logRecovering() ? FLAG_LOG_RECOVERING : 0);
        LittleEndian.putUint64(body, 21, state.logEpoch());
        LittleEndian.putUint64(body, 29, state.lostLogIndex());
        LittleEndian.putUint64(body, 37, state.lostLogTerm());
        return SmallFile.encode(MAGIC, VERSION, body, fileNumber);
    }

    private static Stored read(StorageFileSystem fs, Path path, long fileNumber) throws IOException {
        byte[] body = SmallFile.read(fs, path, MAGIC, VERSION, BODY, fileNumber);
        if (body == null) {
            return null;
        }
        int flags = body[20] & 0xFF;
        long term = LittleEndian.getUint64(body, 8);
        int votedFor = LittleEndian.getInt32(body, 16);
        if ((flags & ~FLAG_LOG_RECOVERING) != 0 || term < 0 || votedFor < HardState.NO_VOTE) {
            return null; // a valid checksum over values this version never writes
        }
        return new Stored(LittleEndian.getUint64(body, 0), new HardState(term, votedFor,
                (flags & FLAG_LOG_RECOVERING) != 0, LittleEndian.getUint64(body, 21),
                LittleEndian.getUint64(body, 29), LittleEndian.getUint64(body, 37)));
    }
}
