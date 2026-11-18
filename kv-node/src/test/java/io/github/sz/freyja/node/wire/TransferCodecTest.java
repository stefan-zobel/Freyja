package io.github.sz.freyja.node.wire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.node.wire.TransferMessage.FileInfo;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.ConfigChange;
import io.github.sz.freyja.raft.RaftMessage;
import io.github.sz.freyja.raft.SnapshotMeta;

/** The encoding of the messages of a snapshot transfer. */
class TransferCodecTest {

    private static List<TransferMessage> messages() {
        ClusterConfig config = ClusterConfig.ofVoters(0, 1, 2).with(new ConfigChange.AddLearner(7));
        return List.of(
                new TransferMessage.Offer(99, 4, new SnapshotMeta(120, 3, config), 2,
                        List.of(new FileInfo("000004.sst", 12_345), new FileInfo("CURRENT", 0))),
                new TransferMessage.Offer(1, 1, new SnapshotMeta(1, 1, ClusterConfig.EMPTY), 1, List.of()),
                new TransferMessage.Fetch(99, 1, 4096),
                new TransferMessage.Chunk(99, 0, 700, new byte[] {1, 2, 3, 4, 5}),
                new TransferMessage.Chunk(99, 0, 0, new byte[0]),
                new TransferMessage.Done(99, true),
                new TransferMessage.Done(99, false),
                new TransferMessage.Abort(Long.MAX_VALUE));
    }

    @Test
    void everyMessageSurvivesEncoding() {
        for (TransferMessage message : messages()) {
            byte[] frame = TransferCodec.encode(3, 5, message);
            assertTrue(TransferCodec.isTransfer(frame));
            assertEquals(5, MessageCodec.addressee(frame));
            TransferCodec.Envelope envelope = TransferCodec.decode(frame);
            assertEquals(3, envelope.from());
            assertEquals(5, envelope.to());
            // A chunk holds an array, which a record compares by identity: compare the bytes.
            assertArrayEquals(frame, TransferCodec.encode(3, 5, envelope.message()));
            assertEquals(message.getClass(), envelope.message().getClass());
            assertEquals(message.transferId(), envelope.message().transferId());
        }
        assertFalse(TransferCodec.isTransfer(MessageCodec.encode(1, 2, new RaftMessage.TimeoutNow(3))));
    }

    @Test
    void whatIsNotOneValidMessageIsRejected() {
        for (TransferMessage message : messages()) {
            byte[] frame = TransferCodec.encode(3, 5, message);
            for (int bit = 0; bit < frame.length * 8; bit++) {
                byte[] flipped = frame.clone();
                flipped[bit / 8] ^= (byte) (1 << (bit % 8));
                assertThrows(DecodeException.class, () -> TransferCodec.decode(flipped));
            }
            for (int length = 0; length < frame.length; length++) {
                byte[] cut = Arrays.copyOf(frame, length);
                assertThrows(DecodeException.class, () -> TransferCodec.decode(cut));
            }
        }
        // A message of the cores is not a transfer message.
        assertThrows(DecodeException.class,
                () -> TransferCodec.decode(MessageCodec.encode(1, 2, new RaftMessage.TimeoutNow(3))));
    }
}
