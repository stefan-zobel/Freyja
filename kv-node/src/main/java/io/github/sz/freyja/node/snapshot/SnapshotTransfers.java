package io.github.sz.freyja.node.snapshot;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import io.github.sz.freyja.lsm.fs.ReadableFile;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.lsm.fs.WritableFile;
import io.github.sz.freyja.node.sm.StateMachineStore;
import io.github.sz.freyja.node.wire.TransferMessage;
import io.github.sz.freyja.node.wire.TransferMessage.Abort;
import io.github.sz.freyja.node.wire.TransferMessage.Chunk;
import io.github.sz.freyja.node.wire.TransferMessage.Done;
import io.github.sz.freyja.node.wire.TransferMessage.Fetch;
import io.github.sz.freyja.node.wire.TransferMessage.FileInfo;
import io.github.sz.freyja.node.wire.TransferMessage.Offer;
import io.github.sz.freyja.raft.SnapshotMeta;

/**
 * Sends and receives snapshots (docs/spec/node.md, section 5).
 *
 * <p><b>Sending.</b> A snapshot to send is a checkpoint of the state machine, taken when the
 * transfer starts and kept in {@code snap/out-N} until the transfer ends or times out. The
 * number of transfers that run at one time is bounded.
 *
 * <p><b>Receiving.</b> A snapshot is received into the directory of a new generation of the
 * state machine, file by file, in chunks the receiver asks for. A request that gets no answer
 * is repeated; a transfer without progress for too long is given up. When every file is
 * there, the generation is verified by its own checksums before anyone is told about it.
 *
 * <p>Time is counted in ticks. An instance is confined to one thread.
 */
public final class SnapshotTransfers implements Closeable {

    /** Subdirectory of the data directory that holds the checkpoints being sent. */
    public static final String DIRECTORY = "snap";

    /**
     * @param chunkSize    the largest number of bytes in one chunk
     * @param resendTicks  a request without an answer is repeated after this many ticks
     * @param timeoutTicks a transfer without progress for this many ticks is given up
     * @param maxOutgoing  how many snapshots are sent at one time
     */
    public record Options(int chunkSize, int resendTicks, int timeoutTicks, int maxOutgoing) {
        public Options {
            if (chunkSize < 1 || resendTicks < 1 || timeoutTicks <= resendTicks || maxOutgoing < 1) {
                throw new IllegalArgumentException("chunk size " + chunkSize + ", resend after " + resendTicks
                        + ", timeout after " + timeoutTicks + ", outgoing " + maxOutgoing);
            }
        }

        /** The defaults: chunks of 1 MiB, repeat after 5 ticks, give up after 100, 2 at a time. */
        public static Options defaults() {
            return new Options(1 << 20, 5, 100, 2);
        }
    }

    /** What the transfers need from the node. */
    public interface Host {

        void send(int toNode, TransferMessage message);

        /** Whether the node has any use for the snapshot a leader offers. */
        boolean wanted(int fromNode, Offer offer);

        /** A snapshot is complete and verified; it waits in {@code generation}. */
        void received(int fromNode, Offer offer, long generation) throws IOException;

        /** A transfer that the node was asked to make has ended. */
        void sendFinished(int toNode, boolean success) throws IOException;
    }

    private static final class Outgoing {
        final int to;
        final Path directory;
        final Offer offer;
        int openFile = -1;
        ReadableFile reader;
        int idle;
        /** Whether the receiver has asked for anything yet. */
        boolean started;
        /** Whether the last byte was sent; the receiver owes its verdict. */
        boolean complete;

        Outgoing(int to, Path directory, Offer offer) {
            this.to = to;
            this.directory = directory;
            this.offer = offer;
        }
    }

    private static final class Incoming {
        final int from;
        final Offer offer;
        final long generation;
        int file;
        long offset;
        WritableFile out;
        int idle;

        Incoming(int from, Offer offer, long generation) {
            this.from = from;
            this.offer = offer;
            this.generation = generation;
        }
    }

    private final StorageFileSystem fs;
    private final Path directory;
    private final StateMachineStore store;
    private final Options options;
    private final Host host;
    private final TreeMap<Integer, Outgoing> outgoing = new TreeMap<>();
    private Incoming incoming;
    /** The transfer received last, to repeat its verdict if the sender did not get it. */
    private int doneFrom = -1;
    private Done done;
    private long checkpoints;

    private long sent;
    private long received;
    private long failed;

    private SnapshotTransfers(StorageFileSystem fs, Path directory, StateMachineStore store, Options options,
                              Host host) {
        this.fs = fs;
        this.directory = directory;
        this.store = store;
        this.options = options;
        this.host = host;
    }

    /** Removes what an earlier run left in {@code snap/} and returns the transfers of this run. */
    public static SnapshotTransfers open(StorageFileSystem fs, Path dataDirectory, StateMachineStore store,
                                         Options options, Host host) throws IOException {
        Path directory = dataDirectory.resolve(DIRECTORY);
        fs.createDirectories(directory);
        for (Path path : fs.list(directory)) {
            String name = path.getFileName().toString();
            // Checkpoints that an earlier run took to send or to back up.
            if (name.startsWith("out-") || name.startsWith("backup-")) {
                StateMachineStore.removeTree(fs, path);
            }
        }
        return new SnapshotTransfers(fs, directory, store, options, host);
    }

    // ------------------------------------------------------------------ sending

    /**
     * Starts to send a snapshot of the state machine as it is now to {@code toNode}.
     *
     * @param leaderTerm the term of the leader that sends
     */
    public void send(int toNode, long leaderTerm) throws IOException {
        Outgoing running = outgoing.remove(toNode);
        if (running != null) {
            // Asked again for the same node: what runs belongs to an earlier leadership.
            host.send(toNode, new Abort(running.offer.transferId()));
            release(running);
        }
        if (outgoing.size() >= options.maxOutgoing()) {
            failed++;
            host.sendFinished(toNode, false);
            return;
        }
        long number = ++checkpoints;
        Path target = directory.resolve(String.format("out-%010d", number));
        SnapshotMeta meta = store.meta();
        int clusterVersion = store.machine().clusterVersion();
        store.checkpoint(target);
        List<FileInfo> files = new ArrayList<>();
        for (Path path : fs.list(target)) {
            files.add(new FileInfo(path.getFileName().toString(), fs.size(path)));
        }
        // Unique among the transfers of this node also across its restarts within a term,
        // as far as a receiver can tell: an offer is the same one only if all of it is equal.
        long id = (leaderTerm << 24) | (number & 0xFFFFFF);
        Outgoing transfer = new Outgoing(toNode, target, new Offer(id, leaderTerm, meta, clusterVersion, files));
        outgoing.put(toNode, transfer);
        host.send(toNode, transfer.offer);
    }

    private void fetch(int from, Fetch fetch) throws IOException {
        Outgoing transfer = outgoing.get(from);
        if (transfer == null || transfer.offer.transferId() != fetch.transferId()) {
            host.send(from, new Abort(fetch.transferId()));
            return;
        }
        List<FileInfo> files = transfer.offer.files();
        if (fetch.file() >= files.size() || fetch.offset() >= files.get(fetch.file()).size()) {
            return; // nothing a receiver that follows the protocol asks for
        }
        FileInfo info = files.get(fetch.file());
        if (transfer.openFile != fetch.file()) {
            if (transfer.reader != null) {
                transfer.reader.close();
                transfer.reader = null;
            }
            transfer.reader = fs.open(transfer.directory.resolve(info.name()));
            transfer.openFile = fetch.file();
        }
        byte[] data = new byte[(int) Math.min(options.chunkSize(), info.size() - fetch.offset())];
        transfer.reader.readFully(fetch.offset(), data, 0, data.length);
        transfer.idle = 0;
        transfer.started = true;
        transfer.complete = fetch.file() == files.size() - 1 && fetch.offset() + data.length == info.size();
        host.send(from, new Chunk(fetch.transferId(), fetch.file(), fetch.offset(), data));
    }

    private void finish(Outgoing transfer, boolean success) throws IOException {
        outgoing.remove(transfer.to);
        release(transfer);
        if (success) {
            sent++;
        } else {
            failed++;
        }
        host.sendFinished(transfer.to, success);
    }

    private void release(Outgoing transfer) throws IOException {
        if (transfer.reader != null) {
            transfer.reader.close();
            transfer.reader = null;
        }
        StateMachineStore.removeTree(fs, transfer.directory);
    }

    // ------------------------------------------------------------------ receiving

    private static boolean valid(Offer offer) {
        Set<String> names = new HashSet<>();
        for (FileInfo file : offer.files()) {
            // A name is the name of a file in one directory and nothing else.
            if (!file.name().matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}") || !names.add(file.name())) {
                return false;
            }
        }
        return !offer.files().isEmpty();
    }

    private void offer(int from, Offer offer) throws IOException {
        if (done != null && doneFrom == from && done.transferId() == offer.transferId()) {
            host.send(from, done); // the sender did not get the verdict
            return;
        }
        if (incoming != null) {
            if (incoming.from == from && incoming.offer.equals(offer)) {
                request(incoming); // the sender did not get the request
                return;
            }
            if (incoming.from != from && offer.leaderTerm() <= incoming.offer.leaderTerm()) {
                host.send(from, new Abort(offer.transferId()));
                return;
            }
            drop(incoming);
        }
        if (!valid(offer) || !host.wanted(from, offer)) {
            host.send(from, new Abort(offer.transferId()));
            return;
        }
        incoming = new Incoming(from, offer, store.stage());
        advance(incoming);
    }

    private void chunk(int from, Chunk chunk) throws IOException {
        Incoming transfer = incoming;
        if (transfer == null || transfer.from != from || transfer.offer.transferId() != chunk.transferId()
                || chunk.file() != transfer.file || chunk.offset() != transfer.offset
                || chunk.data().length == 0
                || chunk.data().length > transfer.offer.files().get(transfer.file).size() - transfer.offset) {
            return; // a repeated or a foreign chunk
        }
        transfer.out.append(chunk.data(), 0, chunk.data().length);
        transfer.offset += chunk.data().length;
        transfer.idle = 0;
        advance(transfer);
    }

    /** Closes what is complete and asks for the next bytes, or finishes the transfer. */
    private void advance(Incoming transfer) throws IOException {
        List<FileInfo> files = transfer.offer.files();
        Path target = store.directory(transfer.generation);
        while (transfer.file < files.size()) {
            FileInfo info = files.get(transfer.file);
            if (transfer.out == null) {
                transfer.out = fs.create(target.resolve(info.name()));
            }
            if (transfer.offset < info.size()) {
                request(transfer);
                return;
            }
            transfer.out.sync();
            transfer.out.close();
            transfer.out = null;
            transfer.file++;
            transfer.offset = 0;
        }
        fs.syncDirectory(target);
        incoming = null;
        Offer offer = transfer.offer;
        boolean intact = store.verify(transfer.generation, offer.meta(), offer.clusterVersion());
        doneFrom = transfer.from;
        done = new Done(offer.transferId(), intact);
        host.send(transfer.from, done);
        if (intact) {
            received++;
            host.received(transfer.from, offer, transfer.generation);
        } else {
            failed++;
            store.discard(transfer.generation);
        }
    }

    private void request(Incoming transfer) {
        host.send(transfer.from, new Fetch(transfer.offer.transferId(), transfer.file, transfer.offset));
    }

    private void drop(Incoming transfer) throws IOException {
        if (transfer.out != null) {
            transfer.out.close();
            transfer.out = null;
        }
        store.discard(transfer.generation);
        if (incoming == transfer) {
            incoming = null;
        }
        failed++;
    }

    // ------------------------------------------------------------------ input

    /** A transfer message has arrived. */
    public void handle(int from, TransferMessage message) throws IOException {
        switch (message) {
            case Offer offer -> offer(from, offer);
            case Fetch fetch -> fetch(from, fetch);
            case Chunk chunk -> chunk(from, chunk);
            case Done verdict -> {
                Outgoing transfer = outgoing.get(from);
                if (transfer != null && transfer.offer.transferId() == verdict.transferId()) {
                    finish(transfer, verdict.accepted());
                }
            }
            case Abort abort -> {
                Outgoing transfer = outgoing.get(from);
                if (transfer != null && transfer.offer.transferId() == abort.transferId()) {
                    finish(transfer, false);
                }
                if (incoming != null && incoming.from == from
                        && incoming.offer.transferId() == abort.transferId()) {
                    drop(incoming);
                }
            }
        }
    }

    /** One unit of time has passed. */
    public void tick() throws IOException {
        for (Iterator<Outgoing> it = new ArrayList<>(outgoing.values()).iterator(); it.hasNext();) {
            Outgoing transfer = it.next();
            transfer.idle++;
            if (transfer.idle >= options.timeoutTicks()) {
                host.send(transfer.to, new Abort(transfer.offer.transferId()));
                finish(transfer, false);
            } else if ((!transfer.started || transfer.complete) && transfer.idle % options.resendTicks() == 0) {
                host.send(transfer.to, transfer.offer);
            }
        }
        if (incoming != null) {
            incoming.idle++;
            if (incoming.idle >= options.timeoutTicks()) {
                // The sender is told, so that it does not keep its checkpoint for nothing.
                host.send(incoming.from, new Abort(incoming.offer.transferId()));
                drop(incoming);
            } else if (incoming.idle % options.resendTicks() == 0) {
                request(incoming);
            }
        }
    }

    // ------------------------------------------------------------------ status

    /** The number of snapshots being sent. */
    public int outgoingCount() {
        return outgoing.size();
    }

    /** Whether a snapshot is being received. */
    public boolean receiving() {
        return incoming != null;
    }

    /** Snapshots sent and accepted by their receiver. */
    public long sentCount() {
        return sent;
    }

    /** Snapshots received completely and found intact. */
    public long receivedCount() {
        return received;
    }

    /** Transfers given up, refused, or found damaged, in either direction. */
    public long failedCount() {
        return failed;
    }

    @Override
    public void close() throws IOException {
        IOException first = null;
        for (Outgoing transfer : outgoing.values()) {
            if (transfer.reader != null) {
                try {
                    transfer.reader.close();
                } catch (IOException e) {
                    first = first == null ? e : first;
                }
                transfer.reader = null;
            }
        }
        outgoing.clear();
        if (incoming != null && incoming.out != null) {
            try {
                incoming.out.close();
            } catch (IOException e) {
                first = first == null ? e : first;
            }
        }
        incoming = null;
        if (first != null) {
            throw first;
        }
    }
}
