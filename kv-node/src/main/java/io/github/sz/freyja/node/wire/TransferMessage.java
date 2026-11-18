package io.github.sz.freyja.node.wire;

import java.util.List;
import java.util.Objects;

import io.github.sz.freyja.raft.SnapshotMeta;

/**
 * The messages that move a snapshot from one node to another (docs/spec/node.md, section 5).
 * The receiver drives the transfer: it asks for every chunk by file and offset, so a lost
 * message costs a repeated request and nothing else.
 */
public sealed interface TransferMessage {

    /** Identifies the transfer among those of its sender. */
    long transferId();

    /** A file of a snapshot. */
    record FileInfo(String name, long size) {
        public FileInfo {
            Objects.requireNonNull(name, "name");
        }
    }

    /**
     * The sender has a snapshot for the receiver.
     *
     * @param leaderTerm     the term of the leader that sends
     * @param meta           what the snapshot covers
     * @param clusterVersion the cluster version as of the snapshot
     * @param files          the files of the snapshot, in the order they are asked for
     */
    record Offer(long transferId, long leaderTerm, SnapshotMeta meta, int clusterVersion, List<FileInfo> files)
            implements TransferMessage {
        public Offer {
            Objects.requireNonNull(meta, "meta");
            files = List.copyOf(files);
        }
    }

    /** The receiver asks for the bytes of file number {@code file} from {@code offset} on. */
    record Fetch(long transferId, int file, long offset) implements TransferMessage {
    }

    /** Bytes of a file. The frame around the message carries their checksum. */
    record Chunk(long transferId, int file, long offset, byte[] data) implements TransferMessage {
        public Chunk {
            Objects.requireNonNull(data, "data");
        }
    }

    /** The receiver has every file and has verified the snapshot, or found it damaged. */
    record Done(long transferId, boolean accepted) implements TransferMessage {
    }

    /** One side gives the transfer up. */
    record Abort(long transferId) implements TransferMessage {
    }
}
