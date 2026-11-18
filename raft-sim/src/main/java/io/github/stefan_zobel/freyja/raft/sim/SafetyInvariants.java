package io.github.stefan_zobel.freyja.raft.sim;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import io.github.stefan_zobel.freyja.raft.HardState;
import io.github.stefan_zobel.freyja.raft.LogEntry;
import io.github.stefan_zobel.freyja.raft.RaftMessage;
import io.github.stefan_zobel.freyja.raft.RaftMessage.AppendEntries;
import io.github.stefan_zobel.freyja.raft.RaftMessage.AppendResponse;
import io.github.stefan_zobel.freyja.raft.RaftMessage.Heartbeat;
import io.github.stefan_zobel.freyja.raft.RaftMessage.RequestVote;
import io.github.stefan_zobel.freyja.raft.RaftMessage.VoteResponse;

/**
 * The safety invariants of the Raft implementation (docs/spec/verification.md, sections 2.4
 * and 2.6).
 *
 * <p><b>This file is protected.</b> It is changed only on the explicit instruction of the
 * project owner, never as part of making a failing test pass. Weakening or removing an
 * invariant is never an acceptable fix. Its checksum is recorded in
 * {@code docs/spec/invariants.sha256}, and a test compares the two.
 *
 * <p>Each invariant is a named, side-effect-free predicate over an {@link Observation}. It
 * returns {@code null} if it holds and a description of the violation otherwise. The simulator
 * calls every predicate after every step. A predicate examines what the latest step did
 * against the complete history, and relies on having held after every earlier step; together
 * the calls therefore establish the invariant for the whole run.
 */
public final class SafetyInvariants {

    /** A named invariant. */
    public record Invariant(String name, Function<Observation, String> predicate) {
    }

    /** Every invariant, in the order of the specification. */
    public static final List<Invariant> ALL = List.of(
            new Invariant("ElectionSafety", SafetyInvariants::electionSafety),
            new Invariant("LogMatching", SafetyInvariants::logMatching),
            new Invariant("LeaderCompleteness", SafetyInvariants::leaderCompleteness),
            new Invariant("StateMachineSafety", SafetyInvariants::stateMachineSafety),
            new Invariant("DurabilityBeforeVisibility", SafetyInvariants::durabilityBeforeVisibility));

    private SafetyInvariants() {
    }

    /**
     * Checks every invariant.
     *
     * @throws AssertionError naming the first invariant that does not hold
     */
    public static void checkAll(Observation observation) {
        for (Invariant invariant : ALL) {
            String violation = invariant.predicate().apply(observation);
            if (violation != null) {
                throw new AssertionError(invariant.name() + " violated: " + violation);
            }
        }
    }

    private static boolean same(LogEntry a, LogEntry b) {
        return a.index() == b.index() && a.term() == b.term() && a.type() == b.type()
                && Arrays.equals(a.payload(), b.payload());
    }

    /** Election safety: at most one leader per term. */
    public static String electionSafety(Observation o) {
        for (Observation.Election election : o.electionsThisStep()) {
            Set<Integer> leaders = o.leadersOfTerm().get(election.term());
            if (leaders == null || !leaders.contains(election.node())) {
                return "the election of node " + election.node() + " in term " + election.term()
                        + " is missing from the history";
            }
            if (leaders.size() > 1) {
                return "term " + election.term() + " has the leaders " + leaders;
            }
        }
        return null;
    }

    /**
     * Log matching: if two logs hold an entry with the same index and term, the logs are
     * identical up to that index.
     *
     * <p>Examined for every entry appended in this step: wherever another log holds the same
     * index with the same term, the entries are equal and the entries before them have equal
     * terms. As the invariant held before the step, equal terms at the preceding index mean
     * identical prefixes.
     */
    public static String logMatching(Observation o) {
        for (Observation.Appended appended : o.appendedThisStep()) {
            LogEntry entry = appended.entry();
            int position = (int) entry.index() - 1;
            List<LogEntry> own = o.log(appended.node());
            if (position >= own.size() || own.get(position) != entry) {
                continue; // replaced again within the same step
            }
            for (int other = 0; other < o.nodeCount(); other++) {
                List<LogEntry> log = o.log(other);
                if (other == appended.node() || position >= log.size()
                        || log.get(position).term() != entry.term()) {
                    continue;
                }
                if (!same(log.get(position), entry)) {
                    return "nodes " + appended.node() + " and " + other + " hold different entries "
                            + entry + " and " + log.get(position);
                }
                if (position > 0 && log.get(position - 1).term() != own.get(position - 1).term()) {
                    return "nodes " + appended.node() + " and " + other + " agree on " + entry
                            + " but hold " + own.get(position - 1) + " and " + log.get(position - 1)
                            + " before it";
                }
            }
        }
        return null;
    }

    /**
     * Log matching, examined over the complete logs of all nodes instead of the latest step.
     * It is too slow to run after every step and is not part of {@link #ALL}. The simulator
     * calls it whenever a log changed in another way than by appending (a restart), at
     * intervals, and at the end of a run.
     */
    public static String logMatchingComplete(Observation o) {
        for (int a = 0; a < o.nodeCount(); a++) {
            for (int b = a + 1; b < o.nodeCount(); b++) {
                List<LogEntry> first = o.log(a);
                List<LogEntry> second = o.log(b);
                int i = Math.min(first.size(), second.size()) - 1;
                while (i >= 0 && first.get(i).term() != second.get(i).term()) {
                    i--;
                }
                for (; i >= 0; i--) {
                    if (!same(first.get(i), second.get(i))) {
                        return "nodes " + a + " and " + b + " agree on a later entry but hold "
                                + first.get(i) + " and " + second.get(i);
                    }
                }
            }
        }
        return null;
    }

    /**
     * Leader completeness: an entry that is committed is present in the log of every leader
     * of a later term than the one in which it was committed.
     *
     * <p>Examined for every node elected in this step, against every applied entry whose
     * commit was observed in an earlier term than that of the election.
     */
    public static String leaderCompleteness(Observation o) {
        List<LogEntry> appliedLog = o.appliedLog();
        List<Long> commitTerms = o.commitTerms();
        for (Observation.Election election : o.electionsThisStep()) {
            List<LogEntry> log = o.log(election.node());
            int known = Math.min(appliedLog.size(), commitTerms.size());
            for (int i = 0; i < known && commitTerms.get(i) < election.term(); i++) {
                if (i >= log.size()) {
                    return "leader " + election.node() + " of term " + election.term() + " has "
                            + log.size() + " entries, but " + appliedLog.get(i)
                            + " was committed in term " + commitTerms.get(i) + " or earlier";
                }
                if (!same(appliedLog.get(i), log.get(i))) {
                    return "leader " + election.node() + " of term " + election.term() + " holds "
                            + log.get(i) + " where " + appliedLog.get(i) + " was committed in term "
                            + commitTerms.get(i) + " or earlier";
                }
            }
        }
        return null;
    }

    /** State machine safety: no two nodes apply different entries at the same index. */
    public static String stateMachineSafety(Observation o) {
        List<LogEntry> appliedLog = o.appliedLog();
        for (Observation.Applied applied : o.appliedThisStep()) {
            LogEntry entry = applied.entry();
            if (entry.index() > appliedLog.size()) {
                return "node " + applied.node() + " applied " + entry + " beyond the applied log of "
                        + appliedLog.size() + " entries";
            }
            LogEntry first = appliedLog.get((int) entry.index() - 1);
            if (!same(first, entry)) {
                return "node " + applied.node() + " applied " + entry + " where " + first
                        + " was applied before";
            }
        }
        return null;
    }

    /**
     * Durability before visibility: no message that promises a state is sent before that
     * state was reported persisted.
     *
     * <p>A message of a term promises that its sender has reached that term. A granted vote
     * promises the vote, a request for votes the vote of the candidate for itself, and an
     * append acknowledgment the entries up to the acknowledged index. A sender whose durable
     * term is beyond the term of the message has kept or outlived the promise.
     */
    public static String durabilityBeforeVisibility(Observation o) {
        for (Observation.Released released : o.releasedThisStep()) {
            RaftMessage message = released.message().message();
            boolean promise = switch (message) {
                case VoteResponse m -> m.granted() && !m.preVote();
                case RequestVote m -> !m.preVote();
                case AppendResponse m -> m.success();
                case AppendEntries _ -> true;
                case Heartbeat _ -> true;
                default -> false;
            };
            if (!promise) {
                continue;
            }
            HardState durable = released.durableHardState();
            if (durable.term() < message.term()) {
                return "node " + released.node() + " released " + message
                        + " while its durable term was " + durable.term();
            }
            if (durable.term() > message.term()) {
                continue;
            }
            if (message instanceof VoteResponse
                    && durable.votedFor() != released.message().toNode()) {
                return "node " + released.node() + " released a vote for "
                        + released.message().toNode() + " while its durable vote was "
                        + durable.votedFor();
            }
            if (message instanceof RequestVote && durable.votedFor() != released.node()) {
                return "node " + released.node() + " asked for votes while its own vote was not durable";
            }
            if ((message instanceof AppendEntries || message instanceof Heartbeat)
                    && durable.votedFor() != released.node()) {
                return "node " + released.node() + " acted as leader of term " + message.term()
                        + " while its durable vote was " + durable.votedFor();
            }
            if (message instanceof AppendResponse response
                    && released.durableLastIndex() < response.index()) {
                return "node " + released.node() + " acknowledged index " + response.index()
                        + " while only " + released.durableLastIndex() + " entries were durable";
            }
        }
        return null;
    }

    /** The names of all invariants with their predicates, for tests that go through them. */
    public static Map<String, Function<Observation, String>> byName() {
        Map<String, Function<Observation, String>> map = new java.util.LinkedHashMap<>();
        for (Invariant invariant : ALL) {
            map.put(invariant.name(), invariant.predicate());
        }
        return map;
    }
}
