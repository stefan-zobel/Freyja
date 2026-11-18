package io.github.sz.freyja.common.admin;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import io.github.sz.freyja.common.Checksums;
import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.LittleEndian;
import io.github.sz.freyja.common.admin.AdminRequest.AddLearner;
import io.github.sz.freyja.common.admin.AdminRequest.ClearSpaceAlarm;
import io.github.sz.freyja.common.admin.AdminRequest.Compact;
import io.github.sz.freyja.common.admin.AdminRequest.PromoteLearner;
import io.github.sz.freyja.common.admin.AdminRequest.RemoveMember;
import io.github.sz.freyja.common.admin.AdminRequest.Scrub;
import io.github.sz.freyja.common.admin.AdminRequest.SetAddress;
import io.github.sz.freyja.common.admin.AdminRequest.SetClusterVersion;
import io.github.sz.freyja.common.admin.AdminRequest.Snapshot;
import io.github.sz.freyja.common.admin.AdminRequest.TransferLeadership;
import io.github.sz.freyja.common.admin.AdminResponse.Done;
import io.github.sz.freyja.common.admin.AdminResponse.Member;
import io.github.sz.freyja.common.admin.AdminResponse.Refused;
import io.github.sz.freyja.common.admin.AdminResponse.Status;
import io.github.sz.freyja.common.kv.CommandCodec.In;
import io.github.sz.freyja.common.kv.CommandCodec.Out;

/**
 * The encoding of the messages between an administrator and a node (docs/spec/operations.md,
 * section 1). The frame is that of the client protocol: version ({@code uint8}, 1), type
 * ({@code uint8}), request id ({@code uint64}), body, CRC32C ({@code uint32}); integers are
 * little-endian, a boolean is one byte, a string is a length ({@code uint32}) and UTF-8.
 *
 * <ul>
 *   <li>Requests: 1 status; 2 add learner (node id {@code uint32}, address for peers, address
 *       for clients); 3 promote learner, 4 remove member (node id, force); 5 transfer
 *       leadership (node id);
 *       6 snapshot; 7 scrub; 8 compact; 9 clear space alarm; 10 set cluster version
 *       ({@code uint32}); 11 set address (as 2); 12 backup (the target directory).</li>
 *   <li>Responses: 65 done (detail); 66 refused (reason, leader id {@code uint32}); 67 status
 *       (node id, cluster id {@code uint64}, software, role, term, leader id, last log index,
 *       commit index, applied index, snapshot index, log recovering, cluster version, space
 *       alarm, restarts, snapshots installed, number of members, and per member node id,
 *       voter, address for peers, address for clients, match index {@code uint64} with all
 *       bits set for unknown, and its state as a string).</li>
 * </ul>
 */
public final class AdminCodec {

    public static final int VERSION = 1;
    private static final int CHECKSUM = 4;

    /** A decoded request with its id. */
    public record RequestEnvelope(long requestId, AdminRequest request) {
    }

    /** A decoded response with the id of its request. */
    public record ResponseEnvelope(long requestId, AdminResponse response) {
    }

    private AdminCodec() {
    }

    private static void string(Out out, String text) {
        out.bytes(text.getBytes(StandardCharsets.UTF_8));
    }

    private static String string(In in) {
        return new String(in.bytes(), StandardCharsets.UTF_8);
    }

    private static boolean bool(In in) {
        int value = in.u8();
        if (value > 1) {
            throw new DecodeException("a boolean with the value " + value);
        }
        return value == 1;
    }

    public static byte[] encode(long requestId, AdminRequest request) {
        Out out = new Out();
        out.u8(VERSION);
        out.u8(switch (request) {
            case AdminRequest.Status _ -> 1;
            case AddLearner _ -> 2;
            case PromoteLearner _ -> 3;
            case RemoveMember _ -> 4;
            case TransferLeadership _ -> 5;
            case Snapshot _ -> 6;
            case Scrub _ -> 7;
            case Compact _ -> 8;
            case ClearSpaceAlarm _ -> 9;
            case SetClusterVersion _ -> 10;
            case SetAddress _ -> 11;
            case AdminRequest.Backup _ -> 12;
        });
        out.u64(requestId);
        switch (request) {
            case AddLearner a -> {
                out.u32(a.nodeId());
                string(out, a.peerAddress());
                string(out, a.clientAddress());
            }
            case SetAddress a -> {
                out.u32(a.nodeId());
                string(out, a.peerAddress());
                string(out, a.clientAddress());
            }
            case PromoteLearner p -> {
                out.u32(p.nodeId());
                out.u8(p.force() ? 1 : 0);
            }
            case RemoveMember r -> {
                out.u32(r.nodeId());
                out.u8(r.force() ? 1 : 0);
            }
            case TransferLeadership t -> out.u32(t.nodeId());
            case SetClusterVersion v -> out.u32(v.version());
            case AdminRequest.Backup b -> string(out, b.directory());
            default -> {
            }
        }
        return seal(out);
    }

    /**
     * @throws DecodeException if the bytes are not one valid request of this format version
     */
    public static RequestEnvelope decodeRequest(byte[] frame) {
        In in = open(frame);
        int type = in.u8();
        long requestId = in.u64();
        AdminRequest request = switch (type) {
            case 1 -> new AdminRequest.Status();
            case 2 -> new AddLearner(in.u32(), string(in), string(in));
            case 3 -> new PromoteLearner(in.u32(), bool(in));
            case 4 -> new RemoveMember(in.u32(), bool(in));
            case 5 -> new TransferLeadership(in.u32());
            case 6 -> new Snapshot();
            case 7 -> new Scrub();
            case 8 -> new Compact();
            case 9 -> new ClearSpaceAlarm();
            case 10 -> new SetClusterVersion(in.u32());
            case 11 -> new SetAddress(in.u32(), string(in), string(in));
            case 12 -> new AdminRequest.Backup(string(in));
            default -> throw new DecodeException("unknown request type " + type);
        };
        in.end();
        return new RequestEnvelope(requestId, request);
    }

    public static byte[] encode(long requestId, AdminResponse response) {
        Out out = new Out();
        out.u8(VERSION);
        out.u8(switch (response) {
            case Done _ -> 65;
            case Refused _ -> 66;
            case Status _ -> 67;
        });
        out.u64(requestId);
        switch (response) {
            case Done d -> string(out, d.detail());
            case Refused r -> {
                string(out, r.reason());
                out.u32(r.leaderId());
            }
            case Status s -> {
                out.u32(s.nodeId());
                out.u64(s.clusterId());
                string(out, s.software());
                string(out, s.role());
                out.u64(s.term());
                out.u32(s.leaderId());
                out.u64(s.lastLogIndex());
                out.u64(s.commitIndex());
                out.u64(s.appliedIndex());
                out.u64(s.snapshotIndex());
                out.u8(s.logRecovering() ? 1 : 0);
                out.u32(s.clusterVersion());
                out.u8(s.spaceAlarm() ? 1 : 0);
                out.u64(s.restarts());
                out.u64(s.snapshotsInstalled());
                out.u32(s.members().size());
                for (Member member : s.members()) {
                    out.u32(member.nodeId());
                    out.u8(member.voter() ? 1 : 0);
                    string(out, member.peerAddress());
                    string(out, member.clientAddress());
                    out.u64(member.matchIndex());
                    string(out, member.state());
                }
            }
        }
        return seal(out);
    }

    /**
     * @throws DecodeException if the bytes are not one valid response of this format version
     */
    public static ResponseEnvelope decodeResponse(byte[] frame) {
        In in = open(frame);
        int type = in.u8();
        long requestId = in.u64();
        AdminResponse response = switch (type) {
            case 65 -> new Done(string(in));
            case 66 -> new Refused(string(in), in.u32());
            case 67 -> {
                int nodeId = in.u32();
                long clusterId = in.u64();
                String software = string(in);
                String role = string(in);
                long term = in.u64();
                int leaderId = in.u32();
                long last = in.u64();
                long commit = in.u64();
                long applied = in.u64();
                long snapshot = in.u64();
                boolean recovering = bool(in);
                int clusterVersion = in.u32();
                boolean alarm = bool(in);
                long restarts = in.u64();
                long installed = in.u64();
                int count = in.count(4 + 1 + 4 + 4 + 8 + 4);
                List<Member> members = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    members.add(new Member(in.u32(), bool(in), string(in), string(in), in.u64(), string(in)));
                }
                yield new Status(nodeId, clusterId, software, role, term, leaderId, last, commit, applied, snapshot,
                        recovering, clusterVersion, alarm, members, restarts, installed);
            }
            default -> throw new DecodeException("unknown response type " + type);
        };
        in.end();
        return new ResponseEnvelope(requestId, response);
    }

    private static byte[] seal(Out out) {
        byte[] body = out.buffer.toByteArray();
        byte[] frame = new byte[body.length + CHECKSUM];
        System.arraycopy(body, 0, frame, 0, body.length);
        LittleEndian.putInt32(frame, body.length, Checksums.crc32c(body, 0, body.length));
        return frame;
    }

    private static In open(byte[] frame) {
        if (frame.length < 1 + 1 + 8 + CHECKSUM) {
            throw new DecodeException("a message of " + frame.length + " bytes is too short");
        }
        int end = frame.length - CHECKSUM;
        if (LittleEndian.getInt32(frame, end) != Checksums.crc32c(frame, 0, end)) {
            throw new DecodeException("message checksum mismatch");
        }
        byte[] body = new byte[end];
        System.arraycopy(frame, 0, body, 0, end);
        In in = new In(body);
        int version = in.u8();
        if (version != VERSION) {
            throw new DecodeException("admin protocol version " + version + "; this software speaks version "
                    + VERSION);
        }
        return in;
    }
}
