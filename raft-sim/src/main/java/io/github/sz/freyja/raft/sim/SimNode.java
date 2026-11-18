package io.github.sz.freyja.raft.sim;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.ProtocolState;
import io.github.sz.freyja.raft.RaftConfig;
import io.github.sz.freyja.raft.RaftNode;
import io.github.sz.freyja.raft.Ready;
import io.github.sz.freyja.raft.SnapshotMeta;

/** One node of a simulated cluster with its storage and its state machine. */
public final class SimNode {

    final int id;
    final RaftConfig config;
    final int tickInterval;
    final SimLog store = new SimLog();
    RaftNode raft;

    /** Whether the process runs. A node that is down receives nothing. */
    boolean up = true;
    /** Whether the node was ever started. A spare node starts when it is to join. */
    boolean started = true;
    /** Whether the node was shut down for good after it had left the cluster. */
    boolean retired;
    /** The configuration this node was started with. */
    ClusterConfig initialConfig;
    /** The index of the entry that carries the configuration in force on this node. */
    long configIndex;
    /**
     * Stored entries from this index on cannot be read, until the truncation that the core
     * asks for has reached the disk; 0 if the stored log is intact.
     */
    long damagedFrom;
    /**
     * The stored log went back to a state it once had while the node was down. The node
     * finds that at every start until its hard state says that it is in log-recovering mode.
     */
    boolean logReverted;
    /** Whether the node was in log-recovering mode after its latest step. */
    boolean recovering;

    /** A snapshot of the state machine. */
    record Snapshot(SnapshotMeta meta, long[] registers) {
    }

    /** The newest snapshot this node has written or installed; it is durable. */
    Snapshot snapshot;
    /** A snapshot received from a leader and not yet installed or discarded. */
    Snapshot staged;
    /** An installation that waits for the writes before it: {@code {sequence}} and what to install. */
    long installAfterSequence = -1;
    Snapshot installing;
    boolean installKeepsSuffix;
    boolean installReplacesStateMachine;
    /** The state machine writes a snapshot when it has applied this many entries more. */
    int snapshotEvery = 50;
    /** Counts the starts of the process. Work scheduled for an earlier start is void. */
    long incarnation;
    /** The node processes nothing before this virtual time. */
    long pausedUntil;

    /** Writes handed out by the core and not yet durable, in sequence order. */
    final ArrayDeque<Ready> pendingWrites = new ArrayDeque<>();
    /** Virtual time at which the latest scheduled write completes. */
    long lastPersistTime;
    long persistedSequence;
    long lastReadySequence;
    long lastSeenTerm;

    // ---- the state machine: registers addressed by one byte
    final long[] registers = new long[256];
    /** Number of entries the state machine has applied. */
    long applied;
    /** Checked operations waiting for the entry at an index to be applied here. */
    final Map<Long, Simulation.ClientOperation> awaitingApply = new HashMap<>();
    /** Checked reads waiting for this node to have applied their read index. */
    final List<Simulation.ClientOperation> awaitingRead = new ArrayList<>();

    SimNode(int id, RaftConfig config, int tickInterval) {
        this.id = id;
        this.config = config;
        this.tickInterval = tickInterval;
    }

    public int id() {
        return id;
    }

    public boolean isUp() {
        return up;
    }

    public ProtocolState view() {
        return raft.view();
    }

    public SimLog store() {
        return store;
    }

    public long applied() {
        return applied;
    }
}
