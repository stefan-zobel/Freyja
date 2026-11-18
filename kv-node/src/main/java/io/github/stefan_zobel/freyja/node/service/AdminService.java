package io.github.stefan_zobel.freyja.node.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import io.github.stefan_zobel.freyja.common.DecodeException;
import io.github.stefan_zobel.freyja.common.Version;
import io.github.stefan_zobel.freyja.common.admin.AdminCodec;
import io.github.stefan_zobel.freyja.common.admin.AdminRequest;
import io.github.stefan_zobel.freyja.common.admin.AdminResponse;
import io.github.stefan_zobel.freyja.common.admin.AdminResponse.Done;
import io.github.stefan_zobel.freyja.common.admin.AdminResponse.Member;
import io.github.stefan_zobel.freyja.common.admin.AdminResponse.Refused;
import io.github.stefan_zobel.freyja.common.admin.AdminResponse.Status;
import io.github.stefan_zobel.freyja.common.kv.Command;
import io.github.stefan_zobel.freyja.common.kv.CommandCodec;
import io.github.stefan_zobel.freyja.node.Node;
import io.github.stefan_zobel.freyja.node.NodeListener;
import io.github.stefan_zobel.freyja.node.sm.KvStateMachine;
import io.github.stefan_zobel.freyja.raft.ClusterConfig;
import io.github.stefan_zobel.freyja.raft.ConfigChange;
import io.github.stefan_zobel.freyja.raft.Notification;
import io.github.stefan_zobel.freyja.raft.ProtocolState;
import io.github.stefan_zobel.freyja.raft.RaftEvent;

/**
 * Serves the requests of an administrator on one node (docs/spec/operations.md, section 1).
 *
 * <p>What concerns only this node is done at once: its status, a snapshot, a scrub, a
 * compaction. A change of the cluster is proposed to the core and answered when the core has
 * taken or refused it; taken means that it is in the log of the leader, not that it has taken
 * effect. The status shows the effect.
 *
 * <p>The service runs on the thread of the node and is told what the node reports.
 */
public final class AdminService implements NodeListener {

    /** Request ids of this service for the core; those of the client service start at 1. */
    private static final long FIRST_ID = 1L << 62;
    /** How many ticks a change of the cluster may wait to be proposed. */
    private static final int PROPOSAL_TICKS = 100;

    /** A change of the cluster that waits for the verdict of the core. */
    private record Waiting(long requestId, ClientService.Reply reply, String what) {
    }

    private final Map<Long, Waiting> waiting = new HashMap<>();
    private Node node;
    private long restarts;
    private long nextId = FIRST_ID;

    /** The node this service serves from now on. */
    public void attach(Node node, long restarts) {
        this.node = node;
        this.restarts = restarts;
    }

    /** The node has stopped. Everyone who waits is told. */
    public void detach() {
        node = null;
        List<Waiting> all = new ArrayList<>(waiting.values());
        waiting.clear();
        for (Waiting w : all) {
            send(w, new Refused("the node stopped before it could answer; the change may be in the log", -1));
        }
    }

    /**
     * A request has arrived. The answer goes to {@code reply}, now or later.
     *
     * @throws IOException if storage failed; the node has stopped then
     */
    public void request(byte[] frame, ClientService.Reply reply) throws IOException {
        AdminCodec.RequestEnvelope envelope;
        try {
            envelope = AdminCodec.decodeRequest(frame);
        } catch (DecodeException e) {
            return; // without a request id there is nobody to answer
        }
        long id = envelope.requestId();
        if (node == null || node.stopped()) {
            reply.send(AdminCodec.encode(id, new Refused("the node is not running", -1)));
            return;
        }
        switch (envelope.request()) {
            case AdminRequest.Status _ -> reply.send(AdminCodec.encode(id, status()));
            case AdminRequest.Snapshot _ -> reply.send(AdminCodec.encode(id, node.requestSnapshot()
                    ? new Done("the log is shortened at the next step")
                    : new Refused("the node has too much to do", -1)));
            case AdminRequest.Scrub _ -> {
                int damaged = node.store().machine().engine().scrub();
                reply.send(AdminCodec.encode(id, new Done(damaged == 0 ? "every block is intact"
                        : damaged + " damaged tables; the node rebuilds its state machine")));
            }
            case AdminRequest.Compact _ -> reply.send(AdminCodec.encode(id,
                    new Done(node.store().machine().engine().compact() ? "compacted" : "nothing to compact")));
            case AdminRequest.AddLearner add -> {
                // First where the node listens, then the node: both through the log.
                command(null, new Command.SetAddress(add.nodeId(), add.peerAddress(), add.clientAddress()));
                change(new Waiting(id, reply, "node " + add.nodeId() + " as a learner"),
                        new ConfigChange.AddLearner(add.nodeId()));
            }
            case AdminRequest.PromoteLearner promote -> change(
                    new Waiting(id, reply, "the promotion of node " + promote.nodeId()),
                    new ConfigChange.PromoteLearner(promote.nodeId()));
            case AdminRequest.RemoveMember remove -> {
                ClusterConfig config = node.view().config();
                change(new Waiting(id, reply, "the removal of node " + remove.nodeId()),
                        config.learners().contains(remove.nodeId()) ? new ConfigChange.RemoveLearner(remove.nodeId())
                                : new ConfigChange.RemoveVoter(remove.nodeId()));
            }
            case AdminRequest.TransferLeadership transfer -> {
                ProtocolState state = node.view();
                if (!state.role().name().equals("LEADER")) {
                    reply.send(AdminCodec.encode(id, new Refused("this node is not the leader", state.leaderId())));
                } else if (!node.submit(new RaftEvent.TransferLeadership(nextId++, transfer.nodeId()))) {
                    reply.send(AdminCodec.encode(id, new Refused("the node has too much to do", -1)));
                } else {
                    reply.send(AdminCodec.encode(id, new Done("the hand-over to node " + transfer.nodeId()
                            + " has begun")));
                }
            }
            case AdminRequest.ClearSpaceAlarm _ -> command(new Waiting(id, reply, "the clearing of the alarm"),
                    new Command.ClearAlarm(Command.Alarm.NOSPACE));
            case AdminRequest.SetClusterVersion version -> command(
                    new Waiting(id, reply, "cluster version " + version.version()),
                    new Command.SetClusterVersion(version.version()));
            case AdminRequest.SetAddress address -> command(
                    new Waiting(id, reply, "the address of node " + address.nodeId()),
                    new Command.SetAddress(address.nodeId(), address.peerAddress(), address.clientAddress()));
        }
    }

    /** Proposes a command of an administrator; {@code w} may be {@code null} if nobody waits. */
    private void command(Waiting w, Command command) {
        long id = nextId++;
        if (w != null) {
            waiting.put(id, w);
        }
        // Time stamp 0: the command does not move the time of the state machine.
        if (node.propose(id, CommandCodec.encode(0, command), node.now() + PROPOSAL_TICKS)
                != Node.Admission.ACCEPTED && w != null) {
            waiting.remove(id);
            send(w, new Refused("the node has too much to do", -1));
        }
    }

    private void change(Waiting w, ConfigChange change) {
        long id = nextId++;
        waiting.put(id, w);
        if (!node.submit(new RaftEvent.ProposeConfigChange(id, change))) {
            waiting.remove(id);
            send(w, new Refused("the node has too much to do", -1));
        }
    }

    private Status status() {
        ProtocolState state = node.view();
        KvStateMachine machine = node.store().machine();
        ClusterConfig config = machine.config();
        List<Member> members = new ArrayList<>();
        TreeSet<Integer> ids = new TreeSet<>(config.voters());
        ids.addAll(config.learners());
        for (int id : ids) {
            KvStateMachine.NodeAddress address = machine.addresses().get(id);
            members.add(new Member(id, config.voters().contains(id), address == null ? "" : address.peer(),
                    address == null ? "" : address.client()));
        }
        return new Status(state.nodeId(), node.identity().clusterId(), Version.SOFTWARE, state.role().name(),
                state.term(), state.leaderId(), state.lastLogIndex(), state.commitIndex(), machine.appliedIndex(),
                node.snapshot().lastIncludedIndex(), state.logRecovering(), machine.clusterVersion(),
                machine.alarms().contains(Command.Alarm.NOSPACE), members, restarts, node.snapshotsInstalled());
    }

    private static void send(Waiting w, AdminResponse response) {
        w.reply().send(AdminCodec.encode(w.requestId(), response));
    }

    @Override
    public void notification(Notification notification) {
        switch (notification) {
            case Notification.ProposalAccepted accepted -> {
                Waiting w = waiting.remove(accepted.requestId());
                if (w != null) {
                    send(w, new Done(w.what() + " is in the log at index " + accepted.index()));
                }
            }
            case Notification.ProposalRejected rejected -> {
                Waiting w = waiting.remove(rejected.requestId());
                if (w != null) {
                    send(w, new Refused(w.what() + " was refused: " + switch (rejected.reason()) {
                        case NOT_LEADER -> "this node is not the leader";
                        case OVERLOADED -> "the leader has too much to do";
                        case CONFIG_IN_PROGRESS -> "another change of the membership is not committed yet";
                        case INVALID_CHANGE -> "the change does not fit the present membership";
                        case LEARNER_BEHIND -> "the learner has not caught up yet";
                        case TRANSFER_IN_PROGRESS -> "the leadership is being handed over";
                        case RECOVERING_NODES -> "the change would depend on nodes that are refilling their logs";
                        default -> rejected.reason().name();
                    }, rejected.reason() == Notification.Reason.NOT_LEADER ? rejected.leaderHint() : -1));
                }
            }
            default -> {
            }
        }
    }

    @Override
    public void proposalExpired(long requestId) {
        Waiting w = waiting.remove(requestId);
        if (w != null) {
            send(w, new Refused(w.what() + " was not proposed in time", -1));
        }
    }
}
