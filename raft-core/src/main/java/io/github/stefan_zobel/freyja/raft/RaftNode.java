package io.github.stefan_zobel.freyja.raft;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

import io.github.stefan_zobel.freyja.raft.Notification.ProposalAccepted;
import io.github.stefan_zobel.freyja.raft.Notification.ProposalRejected;
import io.github.stefan_zobel.freyja.raft.Notification.ReadIndexReady;
import io.github.stefan_zobel.freyja.raft.Notification.ReadIndexRejected;
import io.github.stefan_zobel.freyja.raft.Notification.Reason;
import io.github.stefan_zobel.freyja.raft.Notification.RoleChanged;
import io.github.stefan_zobel.freyja.raft.RaftEvent.ClientPropose;
import io.github.stefan_zobel.freyja.raft.RaftEvent.ClientReadIndex;
import io.github.stefan_zobel.freyja.raft.RaftEvent.MessageReceived;
import io.github.stefan_zobel.freyja.raft.RaftEvent.Persisted;
import io.github.stefan_zobel.freyja.raft.RaftEvent.Tick;
import io.github.stefan_zobel.freyja.raft.RaftMessage.AppendEntries;
import io.github.stefan_zobel.freyja.raft.RaftMessage.AppendResponse;
import io.github.stefan_zobel.freyja.raft.RaftMessage.Heartbeat;
import io.github.stefan_zobel.freyja.raft.RaftMessage.HeartbeatResponse;
import io.github.stefan_zobel.freyja.raft.RaftMessage.RequestVote;
import io.github.stefan_zobel.freyja.raft.RaftMessage.VoteResponse;

/**
 * The Raft state machine of one node (docs/spec/raft.md).
 *
 * <p>The node is pure: it has no clock, no randomness of its own, no threads, and performs no
 * I/O. {@link #step} only changes state. The work that follows from it is collected and handed
 * out by {@link #takeReady}.
 *
 * <p><b>Durability before visibility.</b> Entries and hard-state changes that are not yet
 * confirmed durable live in memory. A message that promises something (a granted vote, an
 * acknowledged append, a request for votes) is released only in a {@code Ready} that follows
 * the {@link Persisted} event covering the complete state the node had when the message was
 * created. The node therefore behaves like one that persists instantly and whose promises are
 * merely delayed on the way, and a crash can never take back a promise.
 *
 * <p>An instance is confined to one thread.
 */
public final class RaftNode {

    /** The node id that stands for "no node". */
    public static final int NO_NODE = -1;

    private static final byte[] EMPTY = new byte[0];

    private final RaftConfig config;
    private final int id;
    private final LogView stable;
    private final ClusterConfig cluster;
    /** The other voters, ascending. */
    private final int[] peers;
    private final boolean voter;

    // ---- hard state
    private long term;
    private int votedFor;
    private final boolean logRecovering;
    private boolean hardStateDirty;

    // ---- log
    /** The entries with the indexes {@code durableIndex + 1} through the last index. */
    private final ArrayList<LogEntry> unstable = new ArrayList<>();
    /** Last index that is confirmed durable and equal to the in-memory log. */
    private long durableIndex;
    /** Entries up to this index are durable or were handed out in a {@code Ready}. */
    private long handedOutIndex;
    /** Pending suffix truncation of the stored log, or -1. */
    private long truncateFrom = -1;
    private long commitIndex;
    /** Entries up to this index were handed to the state machine. */
    private long applyOffered;

    // ---- ready bookkeeping
    private long nextSequence = 1;
    /** Sequence of the last {@code Ready} that carried something to persist. */
    private long lastWriteSequence;
    private long persistedSequence;
    /** Unconfirmed writes in sequence order: {@code {sequence, last log index}}. */
    private final ArrayDeque<long[]> writes = new ArrayDeque<>();
    private final ArrayList<Outbound> released = new ArrayList<>();
    /** Messages waiting for a sequence to be persisted, in ascending order of that sequence. */
    private final ArrayDeque<Gated> gated = new ArrayDeque<>();
    private final ArrayList<Notification> notifications = new ArrayList<>();

    // ---- volatile protocol state
    private Role role = Role.FOLLOWER;
    private int leaderId = NO_NODE;
    private int electionElapsed;
    private int heartbeatElapsed;
    private int randomizedElectionTicks;
    private long prngState;
    private RoleChanged lastRoleNotice;

    // ---- candidate state
    private final TreeMap<Integer, Boolean> votes = new TreeMap<>();
    /** The own vote of a candidate counts once this sequence is persisted. */
    private long selfVoteSequence;

    // ---- leader state
    private final TreeMap<Integer, Progress> progress = new TreeMap<>();
    /** Number of the latest heartbeat round started. It never goes back while the node runs. */
    private long heartbeatRound;
    /** The latest heartbeat round that a majority has answered. */
    private long quorumRound;
    /**
     * Read requests waiting for a heartbeat round that started after they arrived, in order
     * of arrival: {@code {request id, read index, round}}.
     */
    private final ArrayDeque<long[]> pendingReads = new ArrayDeque<>();
    private long uncommittedBytes;
    /** Sizes of the entries appended in this leadership and not yet committed: {index, size}. */
    private final ArrayDeque<long[]> uncommitted = new ArrayDeque<>();

    private record Gated(long sequence, Outbound message) {
    }

    /** What a leader knows about one follower. */
    private static final class Progress {
        /** Highest index known to be durable on the follower and equal to the leader's log. */
        long match;
        /** Index of the next entry to send. */
        long next;
        /** Replicating (pipelined) if set; otherwise probing (one message at a time). */
        boolean replicating;
        /** Probing only: a probe is on its way, send nothing until something comes back. */
        boolean probePaused;
        /** Whether the follower was heard from since the last quorum check. */
        boolean recentActive;
        /** The highest commit index sent to the follower in an append. */
        long sentCommit;
        /** The match index at the previous check for stalled replication. */
        long matchAtCheck;
        /** The latest heartbeat round the follower has answered. */
        long ackedRound;
        /** Last index of every append in flight, ascending. */
        final ArrayDeque<Long> inflight = new ArrayDeque<>();

        void probeFrom(long nextIndex) {
            replicating = false;
            probePaused = false;
            next = nextIndex;
            inflight.clear();
        }
    }

    private RaftNode(RaftConfig config, PersistedState state, LogView log, long appliedIndex) {
        this.config = Objects.requireNonNull(config, "config");
        this.stable = Objects.requireNonNull(log, "log");
        this.id = config.nodeId();
        this.cluster = state.config();
        this.voter = cluster.voters().contains(id);
        this.peers = cluster.voters().stream().mapToInt(Integer::intValue).filter(n -> n != id).toArray();
        this.term = state.hardState().term();
        this.votedFor = state.hardState().votedFor();
        this.logRecovering = state.hardState().logRecovering();
        this.durableIndex = log.lastIndex();
        this.handedOutIndex = durableIndex;
        long snapshotIndex = state.snapshot().lastIncludedIndex();
        if (log.firstIndex() != snapshotIndex + 1) {
            throw new IllegalArgumentException("the log starts at " + log.firstIndex()
                    + " but the snapshot ends at " + snapshotIndex);
        }
        if (appliedIndex < snapshotIndex || appliedIndex > durableIndex) {
            throw new IllegalArgumentException("applied index " + appliedIndex + " outside "
                    + snapshotIndex + ".." + durableIndex);
        }
        // Whatever was applied was committed.
        this.commitIndex = appliedIndex;
        this.applyOffered = appliedIndex;
        this.prngState = config.seed();
        resetElectionTimer();
        this.lastRoleNotice = new RoleChanged(role, term, leaderId);
    }

    /**
     * Creates a node from what the runtime read from stable storage.
     *
     * @param appliedIndex index of the last entry the state machine has applied; at least the
     *                     snapshot index. The node hands out entries after it.
     */
    public static RaftNode create(RaftConfig config, PersistedState state, LogView log,
                                  long appliedIndex) {
        return new RaftNode(config, state, log, appliedIndex);
    }

    // ------------------------------------------------------------------ public surface

    /** Processes one event. This changes state only; it performs nothing. */
    public void step(RaftEvent event) {
        switch (event) {
            case Tick _ -> tick();
            case MessageReceived m -> receive(m.fromNode(), m.message());
            case Persisted p -> persisted(p.readySequence());
            case ClientPropose p -> propose(p.requestId(), p.command());
            case ClientReadIndex r -> readIndex(r.requestId());
        }
        RoleChanged now = new RoleChanged(role, term, leaderId);
        if (!now.equals(lastRoleNotice)) {
            lastRoleNotice = now;
            notifications.add(now);
        }
    }

    /** Whether {@link #takeReady} would return any work. */
    public boolean hasReady() {
        return hardStateDirty
                || truncateFrom != -1
                || handedOutIndex < lastIndex()
                || !released.isEmpty()
                || (!gated.isEmpty() && gated.peekFirst().sequence() <= persistedSequence)
                || applyOffered < Math.min(commitIndex, durableIndex)
                || !notifications.isEmpty();
    }

    /** Returns the work the runtime must do next. See {@link Ready} for the contract. */
    public Ready takeReady() {
        long sequence = nextSequence++;

        HardState hardState = null;
        if (hardStateDirty) {
            hardState = new HardState(term, votedFor, logRecovering);
            hardStateDirty = false;
        }
        long truncate = truncateFrom;
        truncateFrom = -1;
        List<LogEntry> entries = List.of();
        long last = lastIndex();
        if (handedOutIndex < last) {
            int from = (int) (handedOutIndex - durableIndex);
            entries = List.copyOf(unstable.subList(from, unstable.size()));
            handedOutIndex = last;
        }
        if (hardState != null || truncate != -1 || !entries.isEmpty()) {
            lastWriteSequence = sequence;
            writes.addLast(new long[] {sequence, last});
        }

        List<Outbound> messages = new ArrayList<>(released);
        released.clear();
        while (!gated.isEmpty() && gated.peekFirst().sequence() <= persistedSequence) {
            messages.add(gated.pollFirst().message());
        }

        long applyFrom = applyOffered + 1;
        long applyTo = Math.min(commitIndex, durableIndex);
        if (applyTo >= applyFrom) {
            applyOffered = applyTo;
        } else {
            applyTo = applyOffered;
        }

        List<Notification> notes = List.copyOf(notifications);
        notifications.clear();
        return new Ready(sequence, hardState, truncate, entries, List.copyOf(messages), applyFrom,
                applyTo, notes);
    }

    /** A read-only view of the protocol state. */
    public ProtocolState view() {
        return new ProtocolState(id, term, role, votedFor, leaderId, lastIndex(), lastTerm(),
                durableIndex, commitIndex, cluster, logRecovering);
    }

    // ------------------------------------------------------------------ the log

    private long lastIndex() {
        return unstable.isEmpty() ? durableIndex : unstable.getLast().index();
    }

    private long lastTerm() {
        return termAt(lastIndex());
    }

    private long termAt(long index) {
        if (index > durableIndex) {
            return unstable.get((int) (index - durableIndex - 1)).term();
        }
        return stable.termAt(index);
    }

    /** The lowest index whose term is known: the one before the first stored entry. */
    private long floorIndex() {
        return stable.firstIndex() - 1;
    }

    /** At least one entry starting at {@code from}, limited by {@code maxBytes}. */
    private List<LogEntry> entriesFrom(long from, int maxBytes) {
        long last = lastIndex();
        List<LogEntry> result = new ArrayList<>();
        long bytes = 0;
        long next = from;
        if (next <= durableIndex) {
            List<LogEntry> stored = stable.entries(next, durableIndex + 1, maxBytes);
            if (stored.isEmpty() || stored.getFirst().index() != next) {
                throw new IllegalStateException("the log view has no entry " + next);
            }
            for (LogEntry entry : stored) {
                bytes += entry.size();
            }
            result.addAll(stored);
            next += stored.size();
            if (next <= durableIndex) {
                return result; // the size limit cut the stored part short
            }
        }
        while (next <= last) {
            LogEntry entry = unstable.get((int) (next - durableIndex - 1));
            if (!result.isEmpty() && bytes + entry.size() > maxBytes) {
                break;
            }
            bytes += entry.size();
            result.add(entry);
            next++;
        }
        return result;
    }

    private void append(LogEntry entry) {
        if (entry.index() != lastIndex() + 1) {
            throw new IllegalStateException("appending " + entry + " after " + lastIndex());
        }
        unstable.add(entry);
    }

    /** Removes every entry from {@code from} on. The entries must not be committed. */
    private void truncateSuffix(long from) {
        if (from <= commitIndex) {
            throw new IllegalStateException("truncation at " + from
                    + " would remove committed entries (commit index " + commitIndex + ")");
        }
        if (from <= handedOutIndex) {
            // The store holds, or will hold, entries that are no longer part of the log.
            truncateFrom = truncateFrom == -1 ? from : Math.min(truncateFrom, from);
            handedOutIndex = from - 1;
        }
        if (from <= durableIndex) {
            unstable.clear();
            durableIndex = from - 1;
        } else {
            int keep = (int) (from - durableIndex - 1);
            unstable.subList(keep, unstable.size()).clear();
        }
        // A write that is still on its way no longer makes the removed entries part of the
        // log when it completes.
        for (long[] write : writes) {
            write[1] = Math.min(write[1], from - 1);
        }
    }

    /** Largest index at or below {@code index} whose term is at most {@code term}. */
    private long findConflictByTerm(long index, long term) {
        long floor = floorIndex();
        long i = Math.min(index, lastIndex());
        while (i > floor && termAt(i) > term) {
            i--;
        }
        return i;
    }

    // ------------------------------------------------------------------ sending

    private void send(int to, RaftMessage message) {
        released.add(new Outbound(to, message));
    }

    /**
     * The sequence of the {@code Ready} whose persistence makes the complete current state
     * durable.
     */
    private long durabilityPoint() {
        boolean dirty = hardStateDirty || truncateFrom != -1 || handedOutIndex < lastIndex();
        return dirty ? nextSequence : lastWriteSequence;
    }

    /** Sends a message that must not leave before the current state is durable. */
    private void sendWhenDurable(int to, RaftMessage message) {
        long point = durabilityPoint();
        if (point <= persistedSequence) {
            send(to, message);
        } else {
            gated.addLast(new Gated(point, new Outbound(to, message)));
        }
    }

    // ------------------------------------------------------------------ events

    private void persisted(long sequence) {
        if (sequence >= nextSequence) {
            throw new IllegalArgumentException("Ready " + sequence + " was never handed out");
        }
        if (sequence <= persistedSequence) {
            return;
        }
        persistedSequence = sequence;
        long durable = durableIndex;
        while (!writes.isEmpty() && writes.peekFirst()[0] <= sequence) {
            durable = writes.pollFirst()[1];
        }
        if (durable > durableIndex) {
            unstable.subList(0, (int) (durable - durableIndex)).clear();
            durableIndex = durable;
        }
        if (role == Role.LEADER) {
            maybeCommit();
        } else if (role == Role.CANDIDATE) {
            checkElection();
        }
    }

    private void tick() {
        if (role == Role.LEADER) {
            electionElapsed++;
            heartbeatElapsed++;
            if (electionElapsed >= config.electionTicks()) {
                electionElapsed = 0;
                if (!quorumActive()) {
                    becomeFollower(term, NO_NODE);
                    return;
                }
                resumeStalledReplication();
            }
            if (heartbeatElapsed >= config.heartbeatTicks()) {
                heartbeatElapsed = 0;
                startHeartbeatRound();
            }
        } else {
            electionElapsed++;
            if (voter && electionElapsed >= randomizedElectionTicks) {
                campaign();
            }
        }
    }

    private void startHeartbeatRound() {
        heartbeatRound++;
        for (int peer : peers) {
            Progress pr = progress.get(peer);
            send(peer, new Heartbeat(term, Math.min(pr.match, commitIndex), heartbeatRound));
        }
    }

    /**
     * ReadIndex: the commit index at the arrival of the request is a valid read index once
     * a majority has answered a heartbeat round that started after the arrival. That majority
     * shows that no leader of a later term existed when the request arrived, so nothing was
     * committed then that this leader does not know to be committed.
     */
    private void readIndex(long requestId) {
        if (role != Role.LEADER) {
            notifications.add(new ReadIndexRejected(requestId, Reason.NOT_LEADER, leaderId));
            return;
        }
        if (termAt(commitIndex) != term) {
            // Entries of earlier leaders may be committed without this leader knowing it.
            notifications.add(new ReadIndexRejected(requestId, Reason.NOT_READY, id));
            return;
        }
        if (peers.length == 0) {
            notifications.add(new ReadIndexReady(requestId, commitIndex));
            return;
        }
        pendingReads.addLast(new long[] {requestId, commitIndex, heartbeatRound + 1});
        if (quorumRound == heartbeatRound) {
            // No round is on its way; otherwise the request shares the next one.
            startHeartbeatRound();
        }
    }

    /** Called when a follower answered a heartbeat: releases the reads a majority confirmed. */
    private void heartbeatAnswered() {
        long[] rounds = new long[peers.length + 1];
        for (int i = 0; i < peers.length; i++) {
            rounds[i] = progress.get(peers[i]).ackedRound;
        }
        rounds[peers.length] = heartbeatRound;
        Arrays.sort(rounds);
        long confirmed = rounds[rounds.length - cluster.quorum()];
        if (confirmed <= quorumRound) {
            return;
        }
        quorumRound = confirmed;
        while (!pendingReads.isEmpty() && pendingReads.peekFirst()[2] <= quorumRound) {
            long[] read = pendingReads.pollFirst();
            notifications.add(new ReadIndexReady(read[0], read[1]));
        }
        if (!pendingReads.isEmpty() && quorumRound == heartbeatRound) {
            startHeartbeatRound(); // for the requests that arrived while the round was under way
        }
    }

    /** CheckQuorum: whether a majority was heard from since the last check. */
    private boolean quorumActive() {
        int active = 1;
        for (Progress pr : progress.values()) {
            if (pr.recentActive) {
                active++;
            }
            pr.recentActive = false;
        }
        return active >= cluster.quorum();
    }

    private void propose(long requestId, byte[] command) {
        if (role != Role.LEADER) {
            notifications.add(new ProposalRejected(requestId, Reason.NOT_LEADER, leaderId));
            return;
        }
        if (uncommittedBytes > 0 && uncommittedBytes + command.length > config.maxUncommittedBytes()) {
            notifications.add(new ProposalRejected(requestId, Reason.OVERLOADED, id));
            return;
        }
        LogEntry entry = appendAsLeader(EntryType.COMMAND, command);
        notifications.add(new ProposalAccepted(requestId, entry.index(), entry.term()));
        for (int peer : peers) {
            sendAppend(peer, false);
        }
    }

    private LogEntry appendAsLeader(EntryType type, byte[] payload) {
        LogEntry entry = new LogEntry(lastIndex() + 1, term, type, payload);
        append(entry);
        uncommitted.addLast(new long[] {entry.index(), entry.size()});
        uncommittedBytes += entry.size();
        return entry;
    }

    // ------------------------------------------------------------------ messages

    private void receive(int from, RaftMessage message) {
        if (from == id || !cluster.voters().contains(from)) {
            return;
        }
        long messageTerm = message.term();
        if (messageTerm > term) {
            if (message instanceof RequestVote request) {
                if (leaderId != NO_NODE && electionElapsed < config.electionTicks()) {
                    // A leader was heard from within the minimum election timeout. The
                    // request comes from a node that lost contact; it must not disturb.
                    return;
                }
                if (!request.preVote()) {
                    becomeFollower(messageTerm, NO_NODE);
                }
            } else if (message instanceof VoteResponse response && response.preVote()
                    && response.granted()) {
                // Carries the term of the election asked about; the term of this node stays.
            } else {
                boolean fromLeader = message instanceof AppendEntries || message instanceof Heartbeat;
                becomeFollower(messageTerm, fromLeader ? from : NO_NODE);
            }
        } else if (messageTerm < term) {
            if (message instanceof AppendEntries || message instanceof Heartbeat) {
                // A deposed leader learns the term from this answer and steps down.
                send(from, new AppendResponse(term, false, 0, 0, 0));
            } else if (message instanceof RequestVote request && request.preVote()) {
                send(from, new VoteResponse(term, true, false));
            }
            return;
        }

        switch (message) {
            case RequestVote m -> handleRequestVote(from, m);
            case VoteResponse m -> handleVoteResponse(from, m);
            case AppendEntries m -> handleAppendEntries(from, m);
            case AppendResponse m -> handleAppendResponse(from, m);
            case Heartbeat m -> handleHeartbeat(from, m);
            case HeartbeatResponse m -> handleHeartbeatResponse(from, m);
        }
    }

    private void handleRequestVote(int from, RequestVote request) {
        long lastTerm = lastTerm();
        boolean upToDate = request.lastLogTerm() > lastTerm
                || (request.lastLogTerm() == lastTerm && request.lastLogIndex() >= lastIndex());
        if (request.preVote()) {
            boolean grant = request.term() > term && upToDate;
            // Nothing is promised: the answer says what this node would do right now.
            send(from, new VoteResponse(grant ? request.term() : term, true, grant));
            return;
        }
        boolean free = votedFor == from || (votedFor == HardState.NO_VOTE && leaderId == NO_NODE);
        if (free && upToDate) {
            if (votedFor != from) {
                votedFor = from;
                hardStateDirty = true;
            }
            electionElapsed = 0;
            sendWhenDurable(from, new VoteResponse(term, false, true));
        } else {
            send(from, new VoteResponse(term, false, false));
        }
    }

    private void handleVoteResponse(int from, VoteResponse response) {
        boolean relevant;
        if (role == Role.PRE_CANDIDATE) {
            // A grant names the election it is for; one for an earlier election is void.
            relevant = response.preVote() && (!response.granted() || response.term() == term + 1);
        } else if (role == Role.CANDIDATE) {
            relevant = !response.preVote() && response.term() == term;
        } else {
            relevant = false;
        }
        if (relevant) {
            votes.putIfAbsent(from, response.granted());
            checkElection();
        }
    }

    private void checkElection() {
        int granted = 0;
        int rejected = 0;
        for (boolean vote : votes.values()) {
            if (vote) {
                granted++;
            } else {
                rejected++;
            }
        }
        if (role == Role.PRE_CANDIDATE || selfVoteSequence <= persistedSequence) {
            granted++;
        }
        if (granted >= cluster.quorum()) {
            if (role == Role.PRE_CANDIDATE) {
                becomeCandidate();
            } else {
                becomeLeader();
            }
        } else if (rejected >= cluster.quorum()) {
            becomeFollower(term, NO_NODE);
        }
    }

    private void handleAppendEntries(int from, AppendEntries request) {
        if (role == Role.LEADER) {
            throw new IllegalStateException("two leaders in term " + term + ": " + id + " and " + from);
        }
        if (role != Role.FOLLOWER) {
            becomeFollower(term, from);
        }
        leaderId = from;
        electionElapsed = 0;

        long prev = request.prevLogIndex();
        List<LogEntry> entries = request.entries();
        if (prev < commitIndex) {
            // Everything up to the commit index is known to equal the log of the leader.
            sendWhenDurable(from, new AppendResponse(term, true, commitIndex, 0, 0));
            return;
        }
        if (prev > lastIndex() || termAt(prev) != request.prevLogTerm()) {
            long hintIndex = findConflictByTerm(prev, request.prevLogTerm());
            send(from, new AppendResponse(term, false, prev, hintIndex, termAt(hintIndex)));
            return;
        }
        for (int i = 0; i < entries.size(); i++) {
            LogEntry entry = entries.get(i);
            if (entry.index() != prev + 1 + i) {
                throw new IllegalArgumentException("entries out of sequence after " + prev + ": " + entry);
            }
            if (entry.index() <= lastIndex()) {
                if (termAt(entry.index()) == entry.term()) {
                    continue;
                }
                truncateSuffix(entry.index());
            }
            append(entry);
        }
        long match = prev + entries.size();
        commitIndex = Math.max(commitIndex, Math.min(request.leaderCommit(), match));
        sendWhenDurable(from, new AppendResponse(term, true, match, 0, 0));
    }

    private void handleHeartbeat(int from, Heartbeat heartbeat) {
        if (role == Role.LEADER) {
            throw new IllegalStateException("two leaders in term " + term + ": " + id + " and " + from);
        }
        if (role != Role.FOLLOWER) {
            becomeFollower(term, from);
        }
        leaderId = from;
        electionElapsed = 0;
        commitIndex = Math.max(commitIndex, Math.min(heartbeat.commit(), lastIndex()));
        send(from, new HeartbeatResponse(term, heartbeat.round()));
    }

    private void handleAppendResponse(int from, AppendResponse response) {
        Progress pr = role == Role.LEADER ? progress.get(from) : null;
        if (pr == null) {
            return;
        }
        pr.recentActive = true;
        if (response.success()) {
            long match = response.index();
            if (match > lastIndex()) {
                throw new IllegalStateException("node " + from + " acknowledged " + match
                        + " beyond the log end " + lastIndex());
            }
            if (match < pr.match) {
                return; // an old answer
            }
            pr.match = match;
            pr.next = Math.max(pr.next, match + 1);
            while (!pr.inflight.isEmpty() && pr.inflight.peekFirst() <= match) {
                pr.inflight.pollFirst();
            }
            if (!pr.replicating) {
                pr.replicating = true;
                pr.probePaused = false;
                pr.next = match + 1;
                pr.inflight.clear();
            }
            maybeCommit();
            sendAppend(from, false);
            return;
        }

        long rejected = response.index();
        if (pr.replicating) {
            if (rejected <= pr.match) {
                return; // an old answer
            }
            pr.probeFrom(pr.match + 1);
        } else {
            if (rejected != pr.next - 1) {
                return; // not the answer to the probe in flight
            }
            long next = rejected;
            if (response.hintTerm() > 0) {
                next = Math.min(next, findConflictByTerm(response.hintIndex(), response.hintTerm()) + 1);
            } else {
                next = Math.min(next, response.hintIndex() + 1);
            }
            pr.probeFrom(Math.max(next, pr.match + 1));
        }
        sendAppend(from, true);
    }

    private void handleHeartbeatResponse(int from, HeartbeatResponse response) {
        Progress pr = role == Role.LEADER ? progress.get(from) : null;
        if (pr == null) {
            return;
        }
        pr.recentActive = true;
        if (response.round() > pr.ackedRound && response.round() <= heartbeatRound) {
            pr.ackedRound = response.round();
            heartbeatAnswered();
        }
    }

    /**
     * Once per election timeout: sends again what may have been lost. A lost message is
     * normally noticed through the rejection of the one after it; this covers the case that
     * nothing follows.
     */
    private void resumeStalledReplication() {
        long last = lastIndex();
        for (int peer : peers) {
            Progress pr = progress.get(peer);
            if (pr.match < last) {
                if (!pr.replicating) {
                    pr.probePaused = false;
                    sendAppend(peer, true);
                } else if (pr.match == pr.matchAtCheck && !pr.inflight.isEmpty()) {
                    // Appends are in flight, but nothing was acknowledged for a whole
                    // election timeout: find the match point again.
                    pr.probeFrom(pr.match + 1);
                    sendAppend(peer, true);
                } else {
                    sendAppend(peer, false);
                }
            }
            pr.matchAtCheck = pr.match;
        }
    }

    // ------------------------------------------------------------------ replication

    /**
     * Sends to {@code to} what flow control allows. An append without entries is sent only if
     * {@code evenIfEmpty} is set or the follower has not been told the current commit index.
     */
    private void sendAppend(int to, boolean evenIfEmpty) {
        Progress pr = progress.get(to);
        boolean force = evenIfEmpty;
        while (true) {
            if (pr.replicating ? pr.inflight.size() >= config.maxInflightAppends() : pr.probePaused) {
                return;
            }
            long last = lastIndex();
            List<LogEntry> entries =
                    pr.next <= last ? entriesFrom(pr.next, config.maxAppendBytes()) : List.of();
            if (entries.isEmpty() && !force && pr.sentCommit >= commitIndex) {
                return;
            }
            long prev = pr.next - 1;
            send(to, new AppendEntries(term, prev, termAt(prev), entries, commitIndex));
            pr.sentCommit = commitIndex;
            force = false;
            if (!pr.replicating) {
                pr.probePaused = true;
                return;
            }
            if (entries.isEmpty()) {
                return;
            }
            pr.next = entries.getLast().index() + 1;
            pr.inflight.addLast(pr.next - 1);
        }
    }

    /** Advances the commit index to what a majority holds durably, if that is in this term. */
    private void maybeCommit() {
        long[] matches = new long[peers.length + 1];
        for (int i = 0; i < peers.length; i++) {
            matches[i] = progress.get(peers[i]).match;
        }
        // The leader counts itself only up to what it has durably stored.
        matches[peers.length] = durableIndex;
        Arrays.sort(matches);
        long candidate = matches[matches.length - cluster.quorum()];
        if (candidate > commitIndex && termAt(candidate) == term) {
            commitIndex = candidate;
            while (!uncommitted.isEmpty() && uncommitted.peekFirst()[0] <= commitIndex) {
                uncommittedBytes -= uncommitted.pollFirst()[1];
            }
            for (int peer : peers) {
                sendAppend(peer, false);
            }
        }
    }

    // ------------------------------------------------------------------ roles

    private void resetElectionTimer() {
        electionElapsed = 0;
        randomizedElectionTicks =
                config.electionTicks() + (int) Long.remainderUnsigned(nextRandom(), config.electionTicks());
    }

    /** SplitMix64. Its state is part of the node state. */
    private long nextRandom() {
        prngState += 0x9E3779B97F4A7C15L;
        long z = prngState;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    private void clearRoleState() {
        for (long[] read : pendingReads) {
            notifications.add(new ReadIndexRejected(read[0], Reason.NOT_LEADER, NO_NODE));
        }
        pendingReads.clear();
        // Rounds of an earlier leadership confirm nothing about a later one.
        quorumRound = heartbeatRound;
        votes.clear();
        progress.clear();
        uncommitted.clear();
        uncommittedBytes = 0;
        heartbeatElapsed = 0;
    }

    private void becomeFollower(long newTerm, int leader) {
        if (newTerm > term) {
            term = newTerm;
            votedFor = HardState.NO_VOTE;
            hardStateDirty = true;
        }
        role = Role.FOLLOWER;
        leaderId = leader;
        clearRoleState();
        resetElectionTimer();
    }

    /** Starts a pre-vote: asks whether an election in the next term could be won. */
    private void campaign() {
        role = Role.PRE_CANDIDATE;
        leaderId = NO_NODE;
        clearRoleState();
        resetElectionTimer();
        RequestVote request = new RequestVote(term + 1, lastIndex(), lastTerm(), true);
        for (int peer : peers) {
            send(peer, request);
        }
        checkElection();
    }

    private void becomeCandidate() {
        role = Role.CANDIDATE;
        leaderId = NO_NODE;
        clearRoleState();
        resetElectionTimer();
        term++;
        votedFor = id;
        hardStateDirty = true;
        // The own vote counts, and the requests leave, once the new term and the vote are
        // durable. A candidate that forgot its candidacy in a crash could otherwise collect
        // votes that were granted for a log it no longer has.
        selfVoteSequence = durabilityPoint();
        RequestVote request = new RequestVote(term, lastIndex(), lastTerm(), false);
        for (int peer : peers) {
            sendWhenDurable(peer, request);
        }
        checkElection();
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = id;
        clearRoleState();
        electionElapsed = 0;
        long next = lastIndex() + 1;
        for (int peer : peers) {
            Progress pr = new Progress();
            pr.next = next;
            pr.recentActive = true;
            progress.put(peer, pr);
        }
        // An entry of the new term: only through it can earlier entries become committed.
        appendAsLeader(EntryType.NOOP, EMPTY);
        for (int peer : peers) {
            sendAppend(peer, false);
        }
        maybeCommit();
    }
}
