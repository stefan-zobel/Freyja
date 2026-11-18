package io.github.sz.freyja.common.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.admin.AdminResponse.Member;

/** The encoding of the messages between an administrator and a node. */
class AdminCodecTest {

    private static List<AdminRequest> requests() {
        return List.of(new AdminRequest.Status(), new AdminRequest.AddLearner(3, "h3:7000", "h3:7001"),
                new AdminRequest.PromoteLearner(3, false), new AdminRequest.RemoveMember(0, true),
                new AdminRequest.TransferLeadership(1), new AdminRequest.Snapshot(), new AdminRequest.Scrub(),
                new AdminRequest.Compact(), new AdminRequest.ClearSpaceAlarm(), new AdminRequest.SetClusterVersion(2),
                new AdminRequest.SetAddress(1, "", "h1:7001"), new AdminRequest.Backup("/var/backups/monday"));
    }

    private static List<AdminResponse> responses() {
        return List.of(new AdminResponse.Done("taken at index 17"), new AdminResponse.Refused("not the leader", 2),
                new AdminResponse.Refused("", -1),
                new AdminResponse.Status(1, 0xC1A5, "0.1.0", "LEADER", 7, 1, 120, 118, 118, 100, false, 1, true,
                        List.of(new Member(0, true, "h0:7000", "h0:7001", 120, Member.OK),
                                new Member(3, false, "", "", -1, Member.UNKNOWN)), 2, 1),
                new AdminResponse.Status(0, 1, "", "FOLLOWER", 0, -1, 0, 0, 0, 0, true, 0, false, List.of(), 0, 0));
    }

    @Test
    void everyRequestAndResponseSurvivesEncoding() {
        for (AdminRequest request : requests()) {
            AdminCodec.RequestEnvelope envelope = AdminCodec.decodeRequest(AdminCodec.encode(9, request));
            assertEquals(9, envelope.requestId());
            assertEquals(request, envelope.request());
        }
        for (AdminResponse response : responses()) {
            AdminCodec.ResponseEnvelope envelope = AdminCodec.decodeResponse(AdminCodec.encode(9, response));
            assertEquals(9, envelope.requestId());
            assertEquals(response, envelope.response());
        }
    }

    @Test
    void whatIsNotOneValidMessageIsRejected() {
        for (AdminRequest request : requests()) {
            byte[] frame = AdminCodec.encode(9, request);
            for (int bit = 0; bit < frame.length * 8; bit++) {
                byte[] flipped = frame.clone();
                flipped[bit / 8] ^= (byte) (1 << (bit % 8));
                assertThrows(DecodeException.class, () -> AdminCodec.decodeRequest(flipped));
            }
            for (int length = 0; length < frame.length; length++) {
                byte[] cut = Arrays.copyOf(frame, length);
                assertThrows(DecodeException.class, () -> AdminCodec.decodeRequest(cut));
            }
            assertThrows(DecodeException.class, () -> AdminCodec.decodeResponse(frame));
        }
        for (AdminResponse response : responses()) {
            byte[] frame = AdminCodec.encode(9, response);
            for (int bit = 0; bit < frame.length * 8; bit++) {
                byte[] flipped = frame.clone();
                flipped[bit / 8] ^= (byte) (1 << (bit % 8));
                assertThrows(DecodeException.class, () -> AdminCodec.decodeResponse(flipped));
            }
        }
    }
}
