package io.github.stefan_zobel.freyja.node.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;

import io.github.stefan_zobel.freyja.common.Clock;
import io.github.stefan_zobel.freyja.common.DecodeException;
import io.github.stefan_zobel.freyja.common.Metrics;
import io.github.stefan_zobel.freyja.common.kv.ClientCodec;
import io.github.stefan_zobel.freyja.common.kv.ClientRequest;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.NotLeader;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Pair;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Pairs;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Reason;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Rejected;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Result;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Value;
import io.github.stefan_zobel.freyja.common.kv.Command;
import io.github.stefan_zobel.freyja.common.kv.CommandCodec;
import io.github.stefan_zobel.freyja.common.kv.CommandResult;
import io.github.stefan_zobel.freyja.node.Node;
import io.github.stefan_zobel.freyja.node.NodeListener;
import io.github.stefan_zobel.freyja.node.sm.KvStateMachine;
import io.github.stefan_zobel.freyja.raft.HardState;
import io.github.stefan_zobel.freyja.raft.LogEntry;
import io.github.stefan_zobel.freyja.raft.Notification;
import io.github.stefan_zobel.freyja.raft.ProtocolState;
import io.github.stefan_zobel.freyja.raft.RaftEvent;
import io.github.stefan_zobel.freyja.raft.Ready;
import io.github.stefan_zobel.freyja.raft.SnapshotMeta;

/**
 * Serves the requests of clients on one node (docs/spec/node.md, sections 4.3, 7 and 8).
 *
 * <p>A request arrives as bytes and is answered with bytes, exactly once or not at all:
 * <ul>
 *   <li>A command is stamped with the clock of this node and proposed with the deadline of
 *       its request. It is answered when this node has applied the entry the core gave it.
 *       If another entry is applied in its place, the command was lost with a leadership,
 *       and the client is sent to the leader.</li>
 *   <li>A linearizable read asks the core for a read index and is answered from the state
 *       machine once that index is applied. A stale read is answered at once.</li>
 *   <li>A request whose time has passed is dropped without an answer: nobody waits for it.</li>
 * </ul>
 *
 * <p>The service is the listener of its node and runs on the thread of the node. Whoever
 * drives the node calls {@link #afterStep} when the node has worked.
 */
public final class ClientService implements NodeListener {

    /** Takes the answer to one request. */
    @FunctionalInterface
    public interface Reply {
        void send(byte[] frame);
    }

    /**
     * @param tickMillis   how long a tick of the node takes; turns the time a request allows
     *                     into a deadline in the time of the node
     * @param maxScanLimit the largest number of pairs one scan returns
     * @param maxWaiting   how many requests may wait for the log or for a read index
     */
    public record Options(int tickMillis, int maxScanLimit, int maxWaiting) {
        public Options {
            if (tickMillis < 1 || maxScanLimit < 1 || maxWaiting < 1) {
                throw new IllegalArgumentException("tick " + tickMillis + " ms, scan limit " + maxScanLimit
                        + ", waiting " + maxWaiting);
            }
        }

        /** The defaults: ticks of 100 ms, 1,000 pairs, 10,000 waiting requests. */
        public static Options defaults() {
            return new Options(100, 1_000, 10_000);
        }
    }

    /** A request that waits for the log or for a read index. */
    private record Waiting(long requestId, Reply reply, long deadline, ClientRequest request, long arrivedNanos) {
    }

    private final Options options;
    private final LongSupplier wallClockMillis;
    private final IntFunction<String> clientAddress;
    private final NodeListener next;
    private Node node;
    /** By the id under which the core knows the request. */
    private final Map<Long, Waiting> waiting = new HashMap<>();
    /** Proposals the core has taken, by the index it gave them: core id and term. */
    private final TreeMap<Long, long[]> accepted = new TreeMap<>();
    /** Reads that have their read index: core id and index. */
    private final List<long[]> reads = new ArrayList<>();
    private long nextId;
    private long sweptAt;
    private Metrics metrics = Metrics.NONE;
    private Clock clock = () -> 0;
    private long answered;
    private long dropped;
    private long invalid;

    /**
     * @param wallClockMillis the time of day of this node; a command is stamped with it
     * @param clientAddress   the address for clients of a node, by its id; empty if not known
     * @param next            a listener that is told everything this one is told, or
     *                        {@code null}
     */
    public ClientService(Options options, LongSupplier wallClockMillis, IntFunction<String> clientAddress,
                         NodeListener next) {
        this.options = options;
        this.wallClockMillis = wallClockMillis;
        this.clientAddress = clientAddress;
        this.next = next;
    }

    /** The node this service serves from now on. It was opened with this service as its listener. */
    public void attach(Node node) {
        this.node = node;
        this.sweptAt = node.now();
    }

    /** The node has stopped. Everyone who waits is told to ask again. */
    public void detach() {
        node = null;
        List<Waiting> all = new ArrayList<>(waiting.values());
        waiting.clear();
        accepted.clear();
        reads.clear();
        for (Waiting w : all) {
            send(w, new Rejected(Reason.UNAVAILABLE));
        }
    }

    // ------------------------------------------------------------------ requests

    /**
     * A request has arrived. The answer goes to {@code reply}, now or later.
     *
     * @throws IOException if the state machine is damaged; the node has stopped then
     */
    public void request(byte[] frame, Reply reply) throws IOException {
        ClientCodec.RequestEnvelope envelope;
        try {
            envelope = ClientCodec.decodeRequest(frame);
        } catch (DecodeException e) {
            invalid++; // without a request id there is nobody to answer
            return;
        }
        if (node == null || node.stopped()) {
            reply.send(ClientCodec.encode(envelope.requestId(), new Rejected(Reason.UNAVAILABLE)));
            return;
        }
        long ticks = Math.max(1, (envelope.timeoutMillis() + options.tickMillis() - 1L) / options.tickMillis());
        Waiting w = new Waiting(envelope.requestId(), reply, node.now() + ticks, envelope.request(),
                clock.nanoTime());
        switch (envelope.request()) {
            case ClientRequest.Execute execute -> execute(w, execute.command());
            case ClientRequest.Get get -> read(w, get.stale());
            case ClientRequest.Scan scan -> {
                if (scan.reverse()) {
                    send(w, new Rejected(Reason.UNSUPPORTED));
                } else {
                    read(w, scan.stale());
                }
            }
        }
    }

    private void execute(Waiting w, Command command) {
        if (command instanceof Command.SetAlarm || command instanceof Command.ClearAlarm
                || command instanceof Command.SetClusterVersion || command instanceof Command.SetAddress) {
            send(w, new Rejected(Reason.INVALID)); // commands of an administrator
            return;
        }
        if (waiting.size() >= options.maxWaiting()) {
            send(w, new Rejected(Reason.OVERLOADED));
            return;
        }
        long id = ++nextId;
        waiting.put(id, w);
        // The node that proposes a command stamps it with its clock (section 4.2).
        Node.Admission admission = node.propose(id, CommandCodec.encode(wallClockMillis.getAsLong(), command),
                w.deadline());
        if (admission != Node.Admission.ACCEPTED) {
            waiting.remove(id);
            send(w, new Rejected(admission == Node.Admission.EXPIRED ? Reason.EXPIRED : Reason.OVERLOADED));
        }
    }

    private void read(Waiting w, boolean stale) throws IOException {
        if (stale) {
            send(w, readNow(w.request()));
            return;
        }
        if (waiting.size() >= options.maxWaiting()) {
            send(w, new Rejected(Reason.OVERLOADED));
            return;
        }
        long id = ++nextId;
        waiting.put(id, w);
        if (!node.submit(new RaftEvent.ClientReadIndex(id))) {
            waiting.remove(id);
            send(w, new Rejected(Reason.OVERLOADED));
        }
    }

    private ClientResponse readNow(ClientRequest request) throws IOException {
        KvStateMachine machine = node.store().machine();
        try {
            if (request instanceof ClientRequest.Get get) {
                KvStateMachine.Versioned stored = machine.get(get.key());
                return stored == null ? new Value(false, new byte[0], 0, machine.appliedIndex())
                        : new Value(true, stored.value(), stored.modIndex(), machine.appliedIndex());
            }
            ClientRequest.Scan scan = (ClientRequest.Scan) request;
            List<Pair> pairs = new ArrayList<>();
            for (KvStateMachine.Pair pair : machine.scan(scan.start().length == 0 ? null : scan.start(),
                    scan.end().length == 0 ? null : scan.end(), Math.min(scan.limit(), options.maxScanLimit()))) {
                pairs.add(new Pair(pair.key(), pair.value(), pair.modIndex()));
            }
            return new Pairs(pairs, machine.appliedIndex());
        } catch (io.github.stefan_zobel.freyja.lsm.CorruptionException e) {
            // Nothing of a damaged state machine is handed out, and it is not used again.
            node.stateMachineDamaged();
            throw e;
        }
    }

    /**
     * The node has worked: reads whose index is applied are answered, and requests that have
     * waited beyond their time are dropped.
     *
     * @throws IOException if the state machine is damaged; the node has stopped then
     */
    public void afterStep() throws IOException {
        if (node == null) {
            return;
        }
        long applied = node.store().appliedIndex();
        long now = node.now();
        for (Iterator<long[]> it = reads.iterator(); it.hasNext();) {
            long[] read = it.next();
            if (read[1] <= applied) {
                it.remove();
                Waiting w = waiting.remove(read[0]);
                if (w != null) {
                    if (w.deadline() < now) {
                        dropped++;
                    } else {
                        send(w, readNow(w.request()));
                    }
                }
            }
        }
        if (now != sweptAt) {
            sweptAt = now;
            // A proposal past its time is dropped by the node; this is for what waits for a
            // read index or for an entry that never comes.
            for (Iterator<Map.Entry<Long, Waiting>> it = waiting.entrySet().iterator(); it.hasNext();) {
                Map.Entry<Long, Waiting> entry = it.next();
                if (entry.getValue().deadline() < now) {
                    it.remove();
                    dropped++;
                }
            }
        }
    }

    private void send(Waiting w, ClientResponse response) {
        answered++;
        switch (response) {
            case Rejected rejected -> metrics.count("client.rejected." + rejected.reason().name().toLowerCase(
                    java.util.Locale.ROOT), 1);
            case NotLeader _ -> metrics.count("client.redirected", 1);
            default -> metrics.duration(w.request() instanceof ClientRequest.Execute ? "client.command"
                    : "client.read", clock.nanoTime() - w.arrivedNanos());
        }
        w.reply().send(ClientCodec.encode(w.requestId(), response));
    }

    /**
     * Where the service reports: how long commands and reads take from their arrival to
     * their answer, and how many requests are refused, by reason. Set before the first
     * request.
     */
    public void measure(Metrics metrics, Clock clock) {
        this.metrics = metrics;
        this.clock = clock;
    }

    private NotLeader notLeader(int leaderHint) {
        int leader = leaderHint >= 0 && leaderHint != node.identity().nodeId() ? leaderHint : -1;
        String address = leader < 0 ? null : clientAddress.apply(leader);
        return new NotLeader(leader, address == null ? "" : address);
    }

    private void rejected(long id, Notification.Reason reason, int leaderHint) {
        Waiting w = waiting.remove(id);
        if (w == null) {
            return;
        }
        send(w, switch (reason) {
            case NOT_LEADER -> notLeader(leaderHint);
            case OVERLOADED -> new Rejected(Reason.OVERLOADED);
            default -> new Rejected(Reason.UNAVAILABLE);
        });
    }

    // ------------------------------------------------------------------ what the node reports

    @Override
    public void notification(Notification notification) {
        switch (notification) {
            case Notification.ProposalAccepted a -> {
                if (waiting.containsKey(a.requestId())) {
                    accepted.put(a.index(), new long[] {a.requestId(), a.term()});
                }
            }
            case Notification.ProposalRejected r -> rejected(r.requestId(), r.reason(), r.leaderHint());
            case Notification.ReadIndexReady r -> {
                if (waiting.containsKey(r.requestId())) {
                    reads.add(new long[] {r.requestId(), r.index()});
                }
            }
            case Notification.ReadIndexRejected r -> rejected(r.requestId(), r.reason(), r.leaderHint());
            default -> {
            }
        }
        if (next != null) {
            next.notification(notification);
        }
    }

    @Override
    public void applied(LogEntry entry, CommandResult result) {
        long[] proposal = accepted.remove(entry.index());
        if (proposal != null) {
            Waiting w = waiting.remove(proposal[0]);
            if (w != null) {
                if (proposal[1] != entry.term() || result == null) {
                    // Another entry took the place: the command was lost with a leadership.
                    send(w, notLeader(node == null ? -1 : node.view().leaderId()));
                } else if (node != null && w.deadline() < node.now()) {
                    dropped++;
                } else {
                    send(w, new Result(result, entry.index()));
                }
            }
        }
        if (next != null) {
            next.applied(entry, result);
        }
    }

    @Override
    public void proposalExpired(long requestId) {
        if (waiting.remove(requestId) != null) {
            dropped++;
        }
        if (next != null) {
            next.proposalExpired(requestId);
        }
    }

    @Override
    public void snapshotInstalled(SnapshotMeta meta) {
        // Entries up to the snapshot are never applied here one by one: whoever waits for one
        // of them asks the leader again, and the session answers from its record.
        List<Waiting> lost = new ArrayList<>();
        for (Iterator<Map.Entry<Long, long[]>> it =
                accepted.headMap(meta.lastIncludedIndex(), true).entrySet().iterator(); it.hasNext();) {
            Waiting w = waiting.remove(it.next().getValue()[0]);
            it.remove();
            if (w != null) {
                lost.add(w);
            }
        }
        for (Waiting w : lost) {
            send(w, notLeader(node == null ? -1 : node.view().leaderId()));
        }
        if (next != null) {
            next.snapshotInstalled(meta);
        }
    }

    @Override
    public boolean observesCore() {
        return next != null && next.observesCore();
    }

    @Override
    public void stepped(RaftEvent event, ProtocolState state) {
        if (next != null) {
            next.stepped(event, state);
        }
    }

    @Override
    public void ready(Ready ready, HardState durableHardState, long durableLastIndex) {
        if (next != null) {
            next.ready(ready, durableHardState, durableLastIndex);
        }
    }

    // ------------------------------------------------------------------ status

    /** The number of requests that wait for the log or for a read index. */
    public int waitingCount() {
        return waiting.size();
    }

    /** Requests answered, with a result or with a refusal. */
    public long answeredCount() {
        return answered;
    }

    /** Requests dropped without an answer because their time had passed. */
    public long droppedCount() {
        return dropped;
    }

    /** Frames that were not a valid request. */
    public long invalidCount() {
        return invalid;
    }
}
