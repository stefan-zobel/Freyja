package io.github.stefan_zobel.freyja.common.kv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.common.DecodeException;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Pair;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Reason;
import io.github.stefan_zobel.freyja.common.kv.Command.Condition;
import io.github.stefan_zobel.freyja.common.kv.Command.Put;

/** The encoding of the messages between a client and a node. */
class ClientCodecTest {

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static List<ClientRequest> requests() {
        return List.of(
                new ClientRequest.Execute(new Command.RegisterSession()),
                new ClientRequest.Execute(new Command.Write(7, 3, List.of(new Put(b("k"), b("v"))))),
                new ClientRequest.Execute(new Command.Conditional(7, 4, List.of(Condition.notExists(b("k"))),
                        List.of(new Put(b("k"), b("v"))), List.of())),
                new ClientRequest.Get(b("key"), false),
                new ClientRequest.Get(new byte[0], true),
                new ClientRequest.Scan(b("a"), b("m"), 100, false, false),
                new ClientRequest.Scan(new byte[0], new byte[0], 1, true, true));
    }

    private static List<ClientResponse> responses() {
        return List.of(
                new ClientResponse.Result(new CommandResult.Written(), 42),
                new ClientResponse.Result(new CommandResult.Failed(CommandResult.Failure.NO_SPACE), 43),
                new ClientResponse.Value(true, b("value"), 9, 12),
                new ClientResponse.Value(false, new byte[0], 0, 12),
                new ClientResponse.Pairs(List.of(new Pair(b("a"), b("1"), 3), new Pair(b("b"), new byte[0], 4)), 12),
                new ClientResponse.Pairs(List.of(), 0),
                new ClientResponse.NotLeader(2, "node-2.example:7001"),
                new ClientResponse.NotLeader(-1, ""),
                new ClientResponse.Rejected(Reason.OVERLOADED),
                new ClientResponse.Rejected(Reason.UNSUPPORTED));
    }

    @Test
    void everyRequestAndResponseSurvivesEncoding() {
        for (ClientRequest request : requests()) {
            byte[] frame = ClientCodec.encode(77, 1500, request);
            ClientCodec.RequestEnvelope envelope = ClientCodec.decodeRequest(frame);
            assertEquals(77, envelope.requestId());
            assertEquals(1500, envelope.timeoutMillis());
            assertEquals(request.getClass(), envelope.request().getClass());
            // Keys and values are arrays, which records compare by identity: compare the bytes.
            assertArrayEquals(frame, ClientCodec.encode(77, 1500, envelope.request()));
        }
        for (ClientResponse response : responses()) {
            byte[] frame = ClientCodec.encode(Long.MAX_VALUE, response);
            ClientCodec.ResponseEnvelope envelope = ClientCodec.decodeResponse(frame);
            assertEquals(Long.MAX_VALUE, envelope.requestId());
            assertEquals(response.getClass(), envelope.response().getClass());
            assertArrayEquals(frame, ClientCodec.encode(Long.MAX_VALUE, envelope.response()));
        }
        assertThrows(IllegalArgumentException.class,
                () -> ClientCodec.encode(1, -1, new ClientRequest.Get(b("k"), false)));
    }

    @Test
    void whatIsNotOneValidMessageIsRejected() {
        for (ClientRequest request : requests()) {
            byte[] frame = ClientCodec.encode(77, 1500, request);
            for (int bit = 0; bit < frame.length * 8; bit++) {
                byte[] flipped = frame.clone();
                flipped[bit / 8] ^= (byte) (1 << (bit % 8));
                assertThrows(DecodeException.class, () -> ClientCodec.decodeRequest(flipped));
            }
            for (int length = 0; length < frame.length; length++) {
                byte[] cut = Arrays.copyOf(frame, length);
                assertThrows(DecodeException.class, () -> ClientCodec.decodeRequest(cut));
            }
            assertThrows(DecodeException.class, () -> ClientCodec.decodeResponse(frame),
                    "a request is not a response");
        }
        for (ClientResponse response : responses()) {
            byte[] frame = ClientCodec.encode(5, response);
            for (int bit = 0; bit < frame.length * 8; bit++) {
                byte[] flipped = frame.clone();
                flipped[bit / 8] ^= (byte) (1 << (bit % 8));
                assertThrows(DecodeException.class, () -> ClientCodec.decodeResponse(flipped));
            }
            assertThrows(DecodeException.class, () -> ClientCodec.decodeRequest(frame),
                    "a response is not a request");
        }
    }
}
