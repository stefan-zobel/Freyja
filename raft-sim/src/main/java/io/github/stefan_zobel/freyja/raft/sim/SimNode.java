package io.github.stefan_zobel.freyja.raft.sim;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import io.github.stefan_zobel.freyja.raft.LogEntry;
import io.github.stefan_zobel.freyja.raft.ProtocolState;
import io.github.stefan_zobel.freyja.raft.RaftNode;
import io.github.stefan_zobel.freyja.raft.Ready;

/** One node of a simulated cluster with its storage and its state machine. */
public final class SimNode {

    final int id;
    final SimLog store = new SimLog();
    RaftNode raft;

    /** Writes handed out by the core and not yet durable, in sequence order. */
    final ArrayDeque<Ready> pendingWrites = new ArrayDeque<>();
    /** The log as the core sees it: the stored log plus every pending write. */
    final List<LogEntry> fullLog = new ArrayList<>();
    /** Virtual time at which the latest scheduled write completes. */
    long lastPersistTime;
    long persistedSequence;
    long lastReadySequence;
    /** Number of entries the state machine has applied. */
    long applied;
    long lastSeenTerm;
    /** The node processes nothing before this virtual time. */
    long pausedUntil;

    SimNode(int id) {
        this.id = id;
    }

    public int id() {
        return id;
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

    /** The log including entries that are not durable yet. */
    public List<LogEntry> fullLog() {
        return fullLog;
    }
}
