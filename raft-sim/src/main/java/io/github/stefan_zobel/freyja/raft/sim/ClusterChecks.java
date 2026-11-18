package io.github.stefan_zobel.freyja.raft.sim;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import io.github.stefan_zobel.freyja.raft.HardState;
import io.github.stefan_zobel.freyja.raft.LogEntry;
import io.github.stefan_zobel.freyja.raft.Outbound;
import io.github.stefan_zobel.freyja.raft.ProtocolState;
import io.github.stefan_zobel.freyja.raft.RaftMessage;
import io.github.stefan_zobel.freyja.raft.RaftMessage.AppendEntries;
import io.github.stefan_zobel.freyja.raft.RaftMessage.AppendResponse;
import io.github.stefan_zobel.freyja.raft.RaftMessage.Heartbeat;
import io.github.stefan_zobel.freyja.raft.RaftMessage.RequestVote;
import io.github.stefan_zobel.freyja.raft.RaftMessage.VoteResponse;
import io.github.stefan_zobel.freyja.raft.Role;

/**
 * The properties the simulator checks while a cluster runs (docs/spec/verification.md,
 * section 2.4). A violated property throws {@link AssertionError} at the step that caused it.
 */
public final class ClusterChecks {

    private final TreeMap<Long, Integer> leaderOfTerm = new TreeMap<>();
    /** The entries applied by any node, by index: the committed log. */
    private final List<LogEntry> committed = new ArrayList<>();
    /**
     * For every index some node has seen as committed: the term of the first node that saw
     * it, at that moment. The entry was committed in that term or an earlier one.
     */
    private final List<Long> commitTerms = new ArrayList<>();

    /** The committed log as observed so far. */
    public List<LogEntry> committed() {
        return committed;
    }

    /** Terms that had a leader, with the leader of each. */
    public TreeMap<Long, Integer> leaderOfTerm() {
        return leaderOfTerm;
    }

    private static AssertionError violation(String message) {
        return new AssertionError(message);
    }

    private static boolean same(LogEntry a, LogEntry b) {
        return a.index() == b.index() && a.term() == b.term() && a.type() == b.type()
                && Arrays.equals(a.payload(), b.payload());
    }

    /**
     * Election safety and leader completeness, at the moment a node becomes leader.
     *
     * <p>Leader completeness speaks about leaders of later terms only: a node may still win
     * the election of a term that the cluster has left behind, and that leader lacks what was
     * committed since. It can commit nothing, and steps down on the first answer it gets.
     */
    void becameLeader(SimNode node, long term) {
        Integer earlier = leaderOfTerm.putIfAbsent(term, node.id);
        if (earlier != null && earlier != node.id) {
            throw violation("election safety: nodes " + earlier + " and " + node.id
                    + " are both leader of term " + term);
        }
        List<LogEntry> log = node.fullLog;
        // An index applied in this very step has no commit term yet; it is checked against
        // the next leader.
        int known = Math.min(committed.size(), commitTerms.size());
        for (int i = 0; i < known && commitTerms.get(i) < term; i++) {
            if (i >= log.size()) {
                throw violation("leader completeness: leader " + node.id + " of term " + term
                        + " has " + log.size() + " entries, but " + committed.get(i)
                        + " was committed in term " + commitTerms.get(i) + " or earlier");
            }
            if (!same(committed.get(i), log.get(i))) {
                throw violation("leader completeness: leader " + node.id + " of term " + term
                        + " has " + log.get(i) + " where " + committed.get(i) + " is committed");
            }
        }
    }

    /**
     * State machine safety: every node applies the same entry at an index, in order.
     *
     * @return whether this is the first time any node applies the index
     */
    boolean applied(SimNode node, LogEntry entry) {
        if (entry.index() != node.applied + 1) {
            throw violation("node " + node.id + " applies " + entry + " after index " + node.applied);
        }
        if (entry.index() <= committed.size()) {
            LogEntry known = committed.get((int) entry.index() - 1);
            if (!same(known, entry)) {
                throw violation("state machine safety: node " + node.id + " applies " + entry
                        + " where " + known + " was applied before");
            }
            return false;
        } else if (entry.index() == committed.size() + 1) {
            committed.add(entry);
            return true;
        } else {
            throw violation("node " + node.id + " applies " + entry + " beyond the committed log");
        }
    }

    /**
     * No message that promises a state leaves before that state is durable. Checked against
     * the stable storage of the sender at the moment the message is released.
     */
    void released(SimNode node, Outbound outbound) {
        RaftMessage message = outbound.message();
        HardState durable = node.store.hardState();
        boolean promise = switch (message) {
            case VoteResponse m -> m.granted() && !m.preVote();
            case RequestVote m -> !m.preVote();
            case AppendResponse m -> m.success();
            case AppendEntries _ -> true;
            case Heartbeat _ -> true;
            default -> false;
        };
        if (!promise) {
            return;
        }
        if (durable.term() < message.term()) {
            throw violation("node " + node.id + " released " + message
                    + " but its durable term is " + durable.term());
        }
        if (durable.term() > message.term()) {
            return; // the node has moved on; the promise of the old term is void or kept
        }
        switch (message) {
            case VoteResponse _ -> {
                if (durable.votedFor() != outbound.toNode()) {
                    throw violation("node " + node.id + " released a vote for " + outbound.toNode()
                            + " but its durable vote is " + durable.votedFor());
                }
            }
            case RequestVote _ -> {
                if (durable.votedFor() != node.id) {
                    throw violation("node " + node.id + " asks for votes but its own vote is not durable");
                }
            }
            case AppendResponse m -> {
                if (node.store.lastIndex() < m.index()) {
                    throw violation("node " + node.id + " acknowledged " + m.index()
                            + " but only " + node.store.lastIndex() + " entries are durable");
                }
            }
            default -> {
            }
        }
    }

    /** Sanity of the state of one node after a step. */
    void afterStep(SimNode node) {
        ProtocolState view = node.view();
        if (view.term() < node.lastSeenTerm) {
            throw violation("node " + node.id + ": the term went back to " + view.term());
        }
        node.lastSeenTerm = view.term();
        while (commitTerms.size() < view.commitIndex()) {
            commitTerms.add(view.term());
        }
        if (view.durableIndex() > view.lastLogIndex() || view.commitIndex() > view.lastLogIndex()) {
            throw violation("node " + node.id + ": inconsistent indexes " + view);
        }
        if (view.durableIndex() > node.store.lastIndex()) {
            throw violation("node " + node.id + " believes " + view.durableIndex()
                    + " entries durable, the store holds " + node.store.lastIndex());
        }
        if (view.role() == Role.LEADER && !Integer.valueOf(node.id).equals(leaderOfTerm.get(view.term()))) {
            throw violation("node " + node.id + " leads term " + view.term() + " unannounced");
        }
    }

    /** Log matching: logs that agree on the term at an index agree on everything before it. */
    void logMatching(List<SimNode> nodes) {
        for (int a = 0; a < nodes.size(); a++) {
            for (int b = a + 1; b < nodes.size(); b++) {
                List<LogEntry> first = nodes.get(a).fullLog;
                List<LogEntry> second = nodes.get(b).fullLog;
                int i = Math.min(first.size(), second.size()) - 1;
                while (i >= 0 && first.get(i).term() != second.get(i).term()) {
                    i--;
                }
                for (; i >= 0; i--) {
                    if (!same(first.get(i), second.get(i))) {
                        throw violation("log matching: nodes " + nodes.get(a).id + " and "
                                + nodes.get(b).id + " differ at " + first.get(i) + " / " + second.get(i));
                    }
                }
            }
        }
    }
}
