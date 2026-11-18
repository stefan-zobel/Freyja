package io.github.sz.freyja.node.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.Version;
import io.github.sz.freyja.common.admin.AdminCodec;
import io.github.sz.freyja.common.admin.AdminRequest;
import io.github.sz.freyja.common.admin.AdminResponse;
import io.github.sz.freyja.common.admin.AdminResponse.Done;
import io.github.sz.freyja.common.admin.AdminResponse.Member;
import io.github.sz.freyja.common.admin.AdminResponse.Refused;
import io.github.sz.freyja.common.admin.AdminResponse.Status;
import io.github.sz.freyja.common.kv.Command;
import io.github.sz.freyja.common.kv.CommandCodec;
import io.github.sz.freyja.common.kv.CommandResult;
import io.github.sz.freyja.node.Node;
import io.github.sz.freyja.node.NodeListener;
import io.github.sz.freyja.node.sm.KvStateMachine;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.ConfigChange;
import io.github.sz.freyja.raft.LogEntry;
import io.github.sz.freyja.raft.Notification;
import io.github.sz.freyja.raft.PeerProgress;
import io.github.sz.freyja.raft.ProtocolState;
import io.github.sz.freyja.raft.RaftEvent;
import io.github.sz.freyja.raft.SnapshotMeta;

/**
 * Serves the requests of an administrator on one node (docs/spec/operations.md, section 1).
 *
 * <p>What concerns only this node is done at once: its status, a snapshot, a scrub, a
 * compaction. A change of the membership is proposed to the core and answered when the core
 * has taken or refused it; taken means that it is in the log of the leader, not that it has
 * taken effect, which the status shows. A command for the state machine (an alarm, the cluster
 * version, an address) is answered when this node has applied it, with what it did.
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
    /** The ids of the waiting requests that are commands: they are answered when applied. */
    private final java.util.Set<Long> commands = new java.util.HashSet<>();
    /** Commands the core has taken, by the index it gave them: who waits, and the term. */
    private final Map<Long, Waiting> taken = new HashMap<>();
    private final Map<Long, Long> takenTerm = new HashMap<>();
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
        all.addAll(taken.values());
        waiting.clear();
        commands.clear();
        taken.clear();
        takenTerm.clear();
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
            case AdminRequest.PromoteLearner promote -> guarded(
                    new Waiting(id, reply, "the promotion of node " + promote.nodeId()),
                    new ConfigChange.PromoteLearner(promote.nodeId()), promote.force());
            case AdminRequest.RemoveMember remove -> {
                ClusterConfig config = node.view().config();
                guarded(new Waiting(id, reply, "the removal of node " + remove.nodeId()),
                        config.learners().contains(remove.nodeId()) ? new ConfigChange.RemoveLearner(remove.nodeId())
                                : new ConfigChange.RemoveVoter(remove.nodeId()), remove.force());
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
            case AdminRequest.Backup _ -> reply.send(AdminCodec.encode(id, new Refused(
                    "a backup copies files and is made beside the node, not on its thread", -1)));
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
            commands.add(id);
        }
        // Time stamp 0: the command does not move the time of the state machine.
        if (node.propose(id, CommandCodec.encode(0, command), node.now() + PROPOSAL_TICKS)
                != Node.Admission.ACCEPTED && w != null) {
            waiting.remove(id);
            commands.remove(id);
            send(w, new Refused("the node has too much to do", -1));
        }
    }

    /**
     * A change of the voters is refused if the voters that are well would not be a majority
     * afterward: such a cluster could not commit anything, not even the way back. Well is a
     * voter that the leader has heard from lately and that is neither refilling its log nor
     * receiving a snapshot.
     */
    private void guarded(Waiting w, ConfigChange change, boolean force) {
        ProtocolState state = node.view();
        if (!force && state.role().name().equals("LEADER")) {
            ClusterConfig after;
            try {
                after = state.config().with(change);
            } catch (RuntimeException e) {
                after = null; // the core says why the change does not fit
            }
            if (after != null) {
                int well = after.voters().contains(state.nodeId()) ? 1 : 0;
                for (PeerProgress peer : node.peers()) {
                    if (after.voters().contains(peer.nodeId()) && peer.active() && !peer.recovering()
                            && !peer.snapshotting()) {
                        well++;
                    }
                }
                if (well < after.quorum()) {
                    send(w, new Refused(w.what() + " was refused: of the " + after.voters().size()
                            + " voters there would be then, only " + well + " are well, and " + after.quorum()
                            + " are needed; force it if it must be", -1));
                    return;
                }
            }
        }
        change(w, change);
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
        Map<Integer, PeerProgress> peers = new HashMap<>();
        for (PeerProgress peer : node.peers()) {
            peers.put(peer.nodeId(), peer);
        }
        boolean leader = state.role().name().equals("LEADER");
        for (int id : ids) {
            KvStateMachine.NodeAddress address = machine.addresses().get(id);
            PeerProgress peer = peers.get(id);
            long match = id == state.nodeId() && leader ? state.durableIndex() : peer == null ? -1 : peer.matchIndex();
            String how = id == state.nodeId() && leader ? Member.OK : peer == null ? Member.UNKNOWN
                    : peer.recovering() ? Member.RECOVERING : peer.snapshotting() ? Member.SNAPSHOT
                    : peer.active() ? Member.OK : Member.SILENT;
            members.add(new Member(id, config.voters().contains(id), address == null ? "" : address.peer(),
                    address == null ? "" : address.client(), match, how));
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
                if (w != null && commands.remove(accepted.requestId())) {
                    // A command says what it did only when it is applied.
                    taken.put(accepted.index(), w);
                    takenTerm.put(accepted.index(), accepted.term());
                } else if (w != null) {
                    send(w, new Done(w.what() + " is in the log at index " + accepted.index()));
                }
            }
            case Notification.ProposalRejected rejected -> {
                Waiting w = waiting.remove(rejected.requestId());
                commands.remove(rejected.requestId());
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
    public void applied(LogEntry entry, CommandResult result) {
        Waiting w = taken.remove(entry.index());
        Long term = takenTerm.remove(entry.index());
        if (w == null) {
            return;
        }
        if (term != entry.term() || result == null) {
            send(w, new Refused(w.what() + " was lost with a change of the leader; ask the leader again",
                    node == null ? -1 : node.view().leaderId()));
        } else if (result instanceof CommandResult.Failed failed) {
            send(w, new Refused(w.what() + " was refused by the state machine: " + switch (failed.failure()) {
                case VERSION_TOO_OLD -> "the cluster version is higher already";
                default -> failed.failure().name();
            }, -1));
        } else {
            send(w, new Done(w.what() + " is applied, at index " + entry.index()));
        }
    }

    @Override
    public void snapshotInstalled(SnapshotMeta meta) {
        for (java.util.Iterator<Map.Entry<Long, Waiting>> it = taken.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Long, Waiting> entry = it.next();
            if (entry.getKey() <= meta.lastIncludedIndex()) {
                it.remove();
                takenTerm.remove(entry.getKey());
                send(entry.getValue(), new Refused(entry.getValue().what()
                        + " may or may not be applied: the node took a snapshot over it; see the status", -1));
            }
        }
    }

    @Override
    public void proposalExpired(long requestId) {
        commands.remove(requestId);
        Waiting w = waiting.remove(requestId);
        if (w != null) {
            send(w, new Refused(w.what() + " was not proposed in time", -1));
        }
    }
}
