package io.github.stefan_zobel.freyja.raft.sim;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.github.stefan_zobel.freyja.raft.ProtocolState;
import io.github.stefan_zobel.freyja.raft.RaftConfig;
import io.github.stefan_zobel.freyja.raft.RaftNode;
import io.github.stefan_zobel.freyja.raft.Ready;

/** One node of a simulated cluster with its storage and its state machine. */
public final class SimNode {

    final int id;
    final RaftConfig config;
    final int tickInterval;
    final SimLog store = new SimLog();
    RaftNode raft;

    /** Whether the process runs. A node that is down receives nothing. */
    boolean up = true;
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
