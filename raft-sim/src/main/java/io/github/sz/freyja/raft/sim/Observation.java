package io.github.sz.freyja.raft.sim;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.HardState;
import io.github.sz.freyja.raft.LogEntry;
import io.github.sz.freyja.raft.Outbound;

/**
 * What an observer of a cluster knows: the logs of all nodes, the history of elections and of
 * applied entries, and what the latest step did. The safety invariants are predicates over
 * this state ({@link SafetyInvariants}).
 *
 * <p>The simulator records into it; tests build broken histories with the same methods. The
 * history grows over a run. The part named "this step" is cleared by {@link #beginStep}; the
 * invariants examine it and rely on having held for everything before.
 */
public final class Observation {

    /** A node won the election of a term. */
    public record Election(int node, long term) {
    }

    /** A node appended an entry to its log. */
    public record Appended(int node, LogEntry entry) {
    }

    /** A node handed an entry to its state machine. */
    public record Applied(int node, LogEntry entry) {
    }

    /**
     * A node released a message.
     *
     * @param durableHardState the hard state in the stable storage of the node at that moment
     * @param durableLastIndex the last log index in its stable storage at that moment
     */
    public record Released(int node, Outbound message, HardState durableHardState,
                           long durableLastIndex) {
    }

    private final ClusterConfig initialConfig;
    private final List<List<LogEntry>> logs = new ArrayList<>();
    private final TreeMap<Long, Set<Integer>> leadersOfTerm = new TreeMap<>();
    private final List<LogEntry> appliedLog = new ArrayList<>();
    private final List<Long> commitTerms = new ArrayList<>();

    private final List<Election> elections = new ArrayList<>();
    private final List<Appended> appended = new ArrayList<>();
    private final List<Applied> applied = new ArrayList<>();
    private final List<Released> released = new ArrayList<>();

    /** An observation of {@code nodes} nodes that all are voters from the start. */
    public Observation(int nodes) {
        this(nodes, allOf(nodes));
    }

    /**
     * @param nodes         the number of nodes that exist or may join; their ids are
     *                      {@code 0..nodes-1}
     * @param initialConfig the configuration the cluster starts with
     */
    public Observation(int nodes, ClusterConfig initialConfig) {
        this.initialConfig = initialConfig;
        for (int i = 0; i < nodes; i++) {
            logs.add(new ArrayList<>());
        }
    }

    private static ClusterConfig allOf(int nodes) {
        Set<Integer> ids = new TreeSet<>();
        for (int i = 0; i < nodes; i++) {
            ids.add(i);
        }
        return new ClusterConfig(ids, Set.of());
    }

    // ------------------------------------------------------------------ reading

    public int nodeCount() {
        return logs.size();
    }

    /** The configuration the cluster started with: the one in force before any CONFIG entry. */
    public ClusterConfig initialConfig() {
        return initialConfig;
    }

    /** The highest index any node has seen as committed. */
    public long committedIndex() {
        return commitTerms.size();
    }

    /** The log of a node as the node sees it, durable or not. Index 1 is element 0. */
    public List<LogEntry> log(int node) {
        return Collections.unmodifiableList(logs.get(node));
    }

    /** Every term that had a leader, with all nodes that led it. */
    public SortedMap<Long, Set<Integer>> leadersOfTerm() {
        return Collections.unmodifiableSortedMap(leadersOfTerm);
    }

    /** For every index: the entry the first node to apply that index applied. */
    public List<LogEntry> appliedLog() {
        return Collections.unmodifiableList(appliedLog);
    }

    /**
     * For every index some node has seen as committed: the term of the first node that saw
     * it, at that moment. The entry was committed in that term or an earlier one.
     */
    public List<Long> commitTerms() {
        return Collections.unmodifiableList(commitTerms);
    }

    public List<Election> electionsThisStep() {
        return Collections.unmodifiableList(elections);
    }

    public List<Appended> appendedThisStep() {
        return Collections.unmodifiableList(appended);
    }

    public List<Applied> appliedThisStep() {
        return Collections.unmodifiableList(applied);
    }

    public List<Released> releasedThisStep() {
        return Collections.unmodifiableList(released);
    }

    // ------------------------------------------------------------------ recording
    //
    // Public for the simulators that feed an observation: the one of this module and the
    // whole-system simulation of kv-node.

    /** Forgets what the previous step did. The history stays. */
    public void beginStep() {
        elections.clear();
        appended.clear();
        applied.clear();
        released.clear();
    }

    public void elected(int node, long term) {
        leadersOfTerm.computeIfAbsent(term, _ -> new TreeSet<>()).add(node);
        elections.add(new Election(node, term));
    }

    /** The log of {@code node} was cut off from {@code fromIndex} on. */
    public void truncated(int node, long fromIndex) {
        List<LogEntry> log = logs.get(node);
        if (fromIndex <= log.size()) {
            log.subList((int) fromIndex - 1, log.size()).clear();
        }
    }

    public void appended(int node, LogEntry entry) {
        logs.get(node).add(entry);
        appended.add(new Appended(node, entry));
    }

    /** Replaces the log of a node, as after a restart. Nothing counts as appended. */
    public void resetLog(int node, List<LogEntry> entries) {
        List<LogEntry> log = logs.get(node);
        log.clear();
        log.addAll(entries);
    }

    public void applied(int node, LogEntry entry) {
        if (entry.index() == appliedLog.size() + 1) {
            appliedLog.add(entry);
        }
        applied.add(new Applied(node, entry));
    }

    public void released(int node, Outbound message, HardState durableHardState, long durableLastIndex) {
        released.add(new Released(node, message, durableHardState, durableLastIndex));
    }

    /** A node sees everything through {@code commitIndex} as committed while in {@code term}. */
    public void sawCommitted(long commitIndex, long term) {
        while (commitTerms.size() < commitIndex) {
            commitTerms.add(term);
        }
    }
}
