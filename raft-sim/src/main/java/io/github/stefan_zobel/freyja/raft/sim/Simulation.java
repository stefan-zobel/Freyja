package io.github.stefan_zobel.freyja.raft.sim;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import io.github.stefan_zobel.freyja.raft.ClusterConfig;
import io.github.stefan_zobel.freyja.raft.EntryType;
import io.github.stefan_zobel.freyja.raft.LogEntry;
import io.github.stefan_zobel.freyja.raft.Notification;
import io.github.stefan_zobel.freyja.raft.Notification.ProposalAccepted;
import io.github.stefan_zobel.freyja.raft.Notification.ProposalRejected;
import io.github.stefan_zobel.freyja.raft.Notification.Reason;
import io.github.stefan_zobel.freyja.raft.Notification.RoleChanged;
import io.github.stefan_zobel.freyja.raft.Outbound;
import io.github.stefan_zobel.freyja.raft.PersistedState;
import io.github.stefan_zobel.freyja.raft.RaftConfig;
import io.github.stefan_zobel.freyja.raft.RaftEvent;
import io.github.stefan_zobel.freyja.raft.RaftEvent.ClientPropose;
import io.github.stefan_zobel.freyja.raft.RaftEvent.MessageReceived;
import io.github.stefan_zobel.freyja.raft.RaftEvent.Persisted;
import io.github.stefan_zobel.freyja.raft.RaftEvent.Tick;
import io.github.stefan_zobel.freyja.raft.RaftNode;
import io.github.stefan_zobel.freyja.raft.Ready;
import io.github.stefan_zobel.freyja.raft.Role;

/**
 * A deterministic simulation of a Raft cluster (docs/spec/verification.md, section 2.4).
 *
 * <p>Virtual time is driven by a priority queue of events. The network is reliable: every
 * message arrives, after a random delay, so messages overtake each other. Writes become
 * durable after a random delay, in order, and one completion may cover several writes. A
 * node may pause: for a while it processes nothing, and then everything that arrived in the
 * meantime at once. A built-in client proposes commands and follows leader hints.
 *
 * <p>Everything random comes from the seed of the options, so a run is reproducible.
 */
public final class Simulation {

    /** Counters that tell what a run exercised. */
    public static final class Stats {
        public long events;
        public long messages;
        public long proposals;
        public long accepted;
        public long rejectedNotLeader;
        public long rejectedOverloaded;
        /** Accepted proposals whose entry was replaced by one of another term. */
        public long lost;
        public long appliedCommands;
        public long truncations;
        public long writes;
        public long batchedWrites;
        public long pauses;

        @Override
        public String toString() {
            return "events=" + events + " messages=" + messages + " proposals=" + proposals
                    + " accepted=" + accepted + " notLeader=" + rejectedNotLeader
                    + " overloaded=" + rejectedOverloaded + " lost=" + lost
                    + " appliedCommands=" + appliedCommands + " truncations=" + truncations
                    + " writes=" + writes + " batchedWrites=" + batchedWrites + " pauses=" + pauses;
        }
    }

    private record Scheduled(long time, long order, Runnable action) implements Comparable<Scheduled> {
        @Override
        public int compareTo(Scheduled other) {
            int byTime = Long.compare(time, other.time);
            return byTime != 0 ? byTime : Long.compare(order, other.order);
        }
    }

    private record Proposal(long index, long term) {
    }

    private final SimOptions options;
    private final Random random;
    private final PriorityQueue<Scheduled> queue = new PriorityQueue<>();
    private final List<SimNode> nodes = new ArrayList<>();
    private final ClusterChecks checks = new ClusterChecks();
    private final Stats stats = new Stats();
    private long now;
    private long order;
    private long traceHash;

    private int minDelay;
    private int maxDelay;
    private int maxPersistDelay;
    private boolean pauses;

    // ---- the client
    private boolean workload = true;
    private long nextCommandId = 1;
    private int knownLeader = RaftNode.NO_NODE;
    private long knownLeaderTerm;
    /** Accepted proposals by command id. */
    private final Map<Long, Proposal> acceptedProposals = new HashMap<>();
    private final Set<Long> appliedCommandIds = new TreeSet<>();

    public Simulation(SimOptions options) {
        this.options = options;
        this.random = new Random(options.seed());
        this.minDelay = options.minDelay();
        this.maxDelay = options.maxDelay();
        this.maxPersistDelay = options.maxPersistDelay();

        Set<Integer> ids = new TreeSet<>();
        for (int i = 0; i < options.nodes(); i++) {
            ids.add(i);
        }
        ClusterConfig cluster = new ClusterConfig(ids, Set.of());
        for (int i = 0; i < options.nodes(); i++) {
            SimNode node = new SimNode(i);
            RaftConfig config = new RaftConfig(i, random.nextLong(), options.electionTicks(),
                    options.heartbeatTicks(), options.maxInflightAppends(), options.maxAppendBytes(),
                    options.maxUncommittedBytes());
            node.raft = RaftNode.create(config, PersistedState.initial(cluster), node.store, 0);
            nodes.add(node);
            schedule(random.nextInt(options.tickMillis()) + 1, () -> tick(node));
        }
        schedule(options.proposalInterval(), this::clientTurn);
        pauses = options.maxPause() > 0;
        schedule(500, this::pauseTurn);
    }

    // ------------------------------------------------------------------ control

    /** Processes every event up to {@code millis} of virtual time from now. */
    public void run(long millis) {
        long end = now + millis;
        while (!queue.isEmpty() && queue.peek().time() <= end) {
            Scheduled next = queue.poll();
            now = next.time();
            next.action().run();
        }
        now = end;
        checks.logMatching(nodes);
    }

    /** Changes how long messages travel and writes take from now on. */
    public void setDelays(int minDelay, int maxDelay, int maxPersistDelay) {
        this.minDelay = minDelay;
        this.maxDelay = maxDelay;
        this.maxPersistDelay = maxPersistDelay;
    }

    /** Switches process pauses on or off. They happen only if the options allow them. */
    public void setPauses(boolean on) {
        pauses = on && options.maxPause() > 0;
    }

    /** Switches the built-in client on or off. */
    public void setWorkload(boolean on) {
        workload = on;
    }

    public long now() {
        return now;
    }

    public List<SimNode> nodes() {
        return nodes;
    }

    public ClusterChecks checks() {
        return checks;
    }

    public Stats stats() {
        return stats;
    }

    public SimOptions options() {
        return options;
    }

    /** A hash over every {@code Ready} of every node in the order they were produced. */
    public long traceHash() {
        return traceHash;
    }

    /** Hands an event to a node and performs the work that follows. */
    public void step(SimNode node, RaftEvent event) {
        node.raft.step(event);
        stats.events++;
        drain(node);
        checks.afterStep(node);
        if (stats.events % 512 == 0) {
            checks.logMatching(nodes);
        }
    }

    /**
     * Liveness after the network has been calm for a while: one leader, every node follows
     * it, and every log is complete, committed, and applied.
     *
     * @throws AssertionError if the cluster is not in that state
     */
    public void checkConverged() {
        String problem = notConverged();
        if (problem != null) {
            throw new AssertionError(problem);
        }
    }

    /** Whether the cluster is in the state {@link #checkConverged} demands. */
    public boolean isConverged() {
        return notConverged() == null;
    }

    private String notConverged() {
        SimNode leader = null;
        for (SimNode node : nodes) {
            if (node.view().role() == Role.LEADER) {
                if (leader != null) {
                    return "two leaders: " + leader.view() + " and " + node.view();
                }
                leader = node;
            }
        }
        if (leader == null) {
            return "no leader: " + nodes.stream().map(SimNode::view).toList();
        }
        long last = leader.view().lastLogIndex();
        for (SimNode node : nodes) {
            var view = node.view();
            if (view.term() != leader.view().term() || view.leaderId() != leader.id
                    || view.lastLogIndex() != last || view.commitIndex() != last
                    || view.durableIndex() != last || node.applied != last) {
                return "node " + node.id + " has not converged: " + view + ", applied "
                        + node.applied + "; leader: " + leader.view();
            }
        }
        if (checks.committed().size() != last) {
            return "committed " + checks.committed().size() + ", log end " + last;
        }
        return null;
    }

    // ------------------------------------------------------------------ the runtime of a node

    private void schedule(long delay, Runnable action) {
        queue.add(new Scheduled(now + delay, order++, action));
    }

    private void tick(SimNode node) {
        schedule(options.tickMillis(), () -> tick(node));
        if (now >= node.pausedUntil) {
            step(node, new Tick()); // a paused node does not make up for missed ticks
        }
    }

    /** Hands an event to a node, or keeps it until the node is no longer paused. */
    private void deliver(SimNode node, RaftEvent event) {
        if (now < node.pausedUntil) {
            schedule(node.pausedUntil - now, () -> deliver(node, event));
        } else {
            step(node, event);
        }
    }

    /** Now and then pauses a node, with a preference for the leader the client knows. */
    private void pauseTurn() {
        schedule(500 + random.nextInt(2500), this::pauseTurn);
        if (!pauses) {
            return;
        }
        int target = knownLeader != RaftNode.NO_NODE && random.nextBoolean()
                ? knownLeader
                : random.nextInt(nodes.size());
        SimNode node = nodes.get(target);
        node.pausedUntil = Math.max(node.pausedUntil, now + 100 + random.nextInt(options.maxPause()));
        stats.pauses++;
    }

    private void drain(SimNode node) {
        while (node.raft.hasReady()) {
            Ready ready = node.raft.takeReady();
            traceHash = traceHash * 31 + node.id;
            traceHash = traceHash * 31 + ready.toString().hashCode();
            if (ready.sequence() != node.lastReadySequence + 1) {
                throw new AssertionError("Ready " + ready.sequence() + " follows " + node.lastReadySequence);
            }
            node.lastReadySequence = ready.sequence();

            if (ready.hasPersistentWork()) {
                if (ready.truncateSuffixFrom() != -1) {
                    stats.truncations++;
                }
                SimLog.applyTo(node.fullLog, ready);
                node.pendingWrites.addLast(ready);
                stats.writes++;
                long done = Math.max(node.lastPersistTime, now + random.nextInt(maxPersistDelay + 1));
                node.lastPersistTime = done;
                long sequence = ready.sequence();
                schedule(done - now, () -> persist(node, sequence));
            }
            // Roles first: a message of a new leader is checked against its announcement.
            for (Notification note : ready.notifications()) {
                if (note instanceof RoleChanged change) {
                    roleChanged(node, change);
                }
            }
            for (Outbound outbound : ready.messages()) {
                checks.released(node, outbound);
                stats.messages++;
                SimNode target = nodes.get(outbound.toNode());
                int delay = minDelay + random.nextInt(maxDelay - minDelay + 1);
                schedule(delay, () -> deliver(target, new MessageReceived(node.id, outbound.message())));
            }
            if (ready.hasEntriesToApply()) {
                if (ready.applyTo() > node.store.lastIndex()) {
                    throw new AssertionError("node " + node.id + " is to apply " + ready.applyTo()
                            + " but only " + node.store.lastIndex() + " entries are durable");
                }
                for (long i = ready.applyFrom(); i <= ready.applyTo(); i++) {
                    LogEntry entry = node.store.get(i);
                    boolean first = checks.applied(node, entry);
                    node.applied = i;
                    if (first) {
                        commandApplied(entry);
                    }
                }
            }
            for (Notification note : ready.notifications()) {
                switch (note) {
                    case ProposalAccepted accepted -> proposalAccepted(accepted);
                    case ProposalRejected rejected -> proposalRejected(rejected);
                    case RoleChanged _ -> {
                    }
                }
            }
        }
    }

    /** A write completes. Now and then the completion covers every write handed out so far. */
    private void persist(SimNode node, long sequence) {
        if (now < node.pausedUntil) {
            schedule(node.pausedUntil - now, () -> persist(node, sequence));
            return;
        }
        if (node.persistedSequence >= sequence) {
            return;
        }
        long through = sequence;
        if (node.pendingWrites.size() > 1 && random.nextInt(3) == 0) {
            through = node.pendingWrites.peekLast().sequence();
            stats.batchedWrites++;
        }
        while (!node.pendingWrites.isEmpty() && node.pendingWrites.peekFirst().sequence() <= through) {
            node.store.store(node.pendingWrites.pollFirst());
        }
        node.persistedSequence = through;
        step(node, new Persisted(through));
    }

    private void roleChanged(SimNode node, RoleChanged change) {
        if (change.role() == Role.LEADER) {
            checks.becameLeader(node, change.term());
            if (change.term() >= knownLeaderTerm) {
                knownLeaderTerm = change.term();
                knownLeader = node.id;
            }
        }
    }

    // ------------------------------------------------------------------ the client

    private void clientTurn() {
        schedule(1 + random.nextInt(2 * options.proposalInterval()), this::clientTurn);
        if (!workload) {
            return;
        }
        long commandId = nextCommandId++;
        int target = knownLeader == RaftNode.NO_NODE || random.nextInt(5) == 0
                ? random.nextInt(nodes.size())
                : knownLeader;
        propose(commandId, target, 3);
    }

    private void propose(long commandId, int target, int attemptsLeft) {
        byte[] command = new byte[8 + random.nextInt(options.maxPayload() + 1)];
        for (int i = 0; i < 8; i++) {
            command[i] = (byte) (commandId >>> (8 * i));
        }
        stats.proposals++;
        // The request id carries the command id and the attempts left for a retry.
        deliver(nodes.get(target), new ClientPropose(commandId * 4 + attemptsLeft, command));
    }

    private static long commandIdOf(LogEntry entry) {
        long id = 0;
        for (int i = 0; i < 8; i++) {
            id |= (entry.payload()[i] & 0xFFL) << (8 * i);
        }
        return id;
    }

    private void proposalAccepted(ProposalAccepted accepted) {
        stats.accepted++;
        long commandId = accepted.requestId() / 4;
        if (acceptedProposals.put(commandId, new Proposal(accepted.index(), accepted.term())) != null) {
            throw new AssertionError("command " + commandId + " was accepted twice");
        }
    }

    private void proposalRejected(ProposalRejected rejected) {
        if (rejected.reason() == Reason.OVERLOADED) {
            stats.rejectedOverloaded++;
            return;
        }
        stats.rejectedNotLeader++;
        long commandId = rejected.requestId() / 4;
        int attemptsLeft = (int) (rejected.requestId() % 4);
        if (attemptsLeft > 0 && rejected.leaderHint() != RaftNode.NO_NODE) {
            int hint = rejected.leaderHint();
            schedule(1 + random.nextInt(maxDelay + 1), () -> propose(commandId, hint, attemptsLeft - 1));
        }
    }

    /** Called when an index is applied for the first time by any node. */
    private void commandApplied(LogEntry entry) {
        if (entry.type() != EntryType.COMMAND) {
            return;
        }
        long commandId = commandIdOf(entry);
        Proposal proposal = acceptedProposals.get(commandId);
        if (proposal == null || proposal.index() != entry.index() || proposal.term() != entry.term()) {
            throw new AssertionError("applied " + entry + " of command " + commandId
                    + ", which was accepted as " + proposal);
        }
        if (!appliedCommandIds.add(commandId)) {
            throw new AssertionError("command " + commandId + " was applied twice");
        }
        stats.appliedCommands++;
    }

    /**
     * Counts the accepted proposals whose place in the log was taken by another entry or
     * dropped. Meaningful once the cluster has converged: then every log ends where the
     * committed log ends.
     */
    public long countLostProposals() {
        long lost = 0;
        List<LogEntry> committed = checks.committed();
        for (Map.Entry<Long, Proposal> accepted : acceptedProposals.entrySet()) {
            Proposal proposal = accepted.getValue();
            if (proposal.index() > committed.size()
                    || committed.get((int) proposal.index() - 1).term() != proposal.term()) {
                lost++;
            }
        }
        stats.lost = lost;
        return lost;
    }

    /** Number of accepted proposals that are neither applied nor lost. */
    public long countUndecidedProposals() {
        return stats.accepted - stats.appliedCommands - countLostProposals();
    }
}
