package io.github.stefan_zobel.freyja.node.sim;

import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;

import io.github.stefan_zobel.freyja.common.kv.CommandResult;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.Failed;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.Failure;
import io.github.stefan_zobel.freyja.node.sim.SystemObservation.Acknowledged;
import io.github.stefan_zobel.freyja.node.sim.SystemObservation.Applied;
import io.github.stefan_zobel.freyja.node.sim.SystemObservation.Digest;

/**
 * The invariants of the whole system (docs/spec/verification.md, sections 2.6 and 3). The
 * whole-system simulation calls every predicate after every step.
 *
 * <p><b>This file is protected.</b> It states what the system must never do. It is changed
 * only on the explicit instruction of the project owner, never to make a failing test pass:
 * an invariant that fails points at a fault in the code. Its checksum is recorded in
 * {@code docs/spec/invariants.sha256}, and a test compares the two.
 *
 * <p>Each predicate looks at what the latest step added to the observation and judges it
 * against the history. It relies on having held after every earlier step. A predicate
 * returns {@code null} if the invariant holds, otherwise a description of the violation.
 */
public final class SystemInvariants {

    /** A named predicate. */
    public record Invariant(String name, Function<SystemObservation, String> predicate) {
    }

    public static final List<Invariant> ALL = List.of(
            new Invariant("StateMachineEquality", SystemInvariants::stateMachineEquality),
            new Invariant("NoAcknowledgedWriteLost", SystemInvariants::noAcknowledgedWriteLost),
            new Invariant("ExactlyOnce", SystemInvariants::exactlyOnce));

    private SystemInvariants() {
    }

    /** Throws an {@link AssertionError} that names the first invariant that does not hold. */
    public static void checkAll(SystemObservation observation) {
        for (Invariant invariant : ALL) {
            String violation = invariant.predicate().apply(observation);
            if (violation != null) {
                throw new AssertionError(invariant.name() + " violated: " + violation);
            }
        }
    }

    /**
     * State-machine equality: nodes that have applied the same entries are in the same state.
     *
     * <ul>
     *   <li>Whatever node applies the entry with a given index applies the same entry and
     *       gets the same result.</li>
     *   <li>Two state machines that have applied everything up to the same index have the
     *       same digest.</li>
     * </ul>
     */
    public static String stateMachineEquality(SystemObservation o) {
        for (Applied applied : o.appliedThisStep()) {
            Applied first = o.firstApplied(applied.index());
            if (first.term() != applied.term() || first.commandHash() != applied.commandHash()) {
                return "node " + applied.node() + " applied another entry at index " + applied.index()
                        + " than node " + first.node() + ": " + applied + " / " + first;
            }
            if (!Objects.equals(first.result(), applied.result())) {
                return "node " + applied.node() + " got another result for entry " + applied.index()
                        + " than node " + first.node() + ": " + applied.result() + " / " + first.result();
            }
        }
        for (Digest digest : o.digestsThisStep()) {
            Digest first = o.firstDigest(digest.index());
            if (first.digest() != digest.digest()) {
                return "after entry " + digest.index() + " node " + digest.node() + " is in another state"
                        + " than node " + first.node() + " was";
            }
        }
        return null;
    }

    /**
     * No acknowledged write is lost: what a client was told stays true.
     *
     * <ul>
     *   <li>An answer names an entry that was applied, with the result the client was told.</li>
     *   <li>No node ever applies another entry at the index of an acknowledged command.</li>
     * </ul>
     */
    public static String noAcknowledgedWriteLost(SystemObservation o) {
        for (Acknowledged ack : o.acknowledgedThisStep()) {
            Applied first = o.firstApplied(ack.index());
            if (first == null) {
                return "client " + ack.client() + " was told a result for entry " + ack.index()
                        + ", which no node has applied";
            }
            if (first.commandHash() != ack.commandHash() || !Objects.equals(first.result(), ack.result())) {
                return "client " + ack.client() + " was told " + ack.result() + " for entry " + ack.index()
                        + ", but the entry applied there is " + first;
            }
        }
        for (Applied applied : o.appliedThisStep()) {
            for (Acknowledged ack : o.acknowledgedAt(applied.index())) {
                if (applied.commandHash() != ack.commandHash()) {
                    return "node " + applied.node() + " applied another entry at index " + ack.index()
                            + " than the one acknowledged to client " + ack.client();
                }
            }
        }
        return null;
    }

    /**
     * Exactly-once: a command of a session takes effect at most once, however often it is in
     * the log, and everyone who asks is told the result of that one time.
     *
     * <p>Along the log, the applications of one command (one session, one sequence number)
     * give: first any number of rejections that do not use the sequence number up
     * ({@code NO_SPACE}); then at most one result of taking effect; after it only that same
     * result again, or {@code SEQUENCE_TOO_OLD} once the session has moved on. From the
     * moment the session is gone, every application gives {@code NO_SUCH_SESSION}.
     */
    public static String exactlyOnce(SystemObservation o) {
        for (Applied latest : o.appliedThisStep()) {
            if (latest.sessionId() == 0) {
                continue;
            }
            // In the order of the log; the same entry applied by several nodes counts once.
            TreeMap<Long, Applied> byIndex = new TreeMap<>();
            for (Applied applied : o.applications(latest.sessionId(), latest.sequence())) {
                byIndex.putIfAbsent(applied.index(), applied);
            }
            CommandResult effect = null;
            boolean gone = false;
            for (Applied applied : byIndex.values()) {
                CommandResult result = applied.result();
                Failure failure = result instanceof Failed failed ? failed.failure() : null;
                if (gone) {
                    if (failure != Failure.NO_SUCH_SESSION) {
                        return "command " + latest.sequence() + " of session " + latest.sessionId()
                                + " gave " + result + " at entry " + applied.index()
                                + " although the session was gone before";
                    }
                } else if (failure == Failure.NO_SUCH_SESSION) {
                    gone = true;
                } else if (effect == null) {
                    if (failure != Failure.NO_SPACE) {
                        // Also SEQUENCE_TOO_OLD: the session moved on before this command was
                        // ever applied, and it never takes effect.
                        effect = result;
                    }
                } else if (!result.equals(effect) && failure != Failure.SEQUENCE_TOO_OLD) {
                    return "command " + latest.sequence() + " of session " + latest.sessionId()
                            + " gave " + result + " at entry " + applied.index() + " after it gave " + effect;
                }
            }
        }
        for (Acknowledged ack : o.acknowledgedThisStep()) {
            if (ack.sessionId() == 0) {
                continue;
            }
            if (isRejection(ack.result())) {
                continue;
            }
            for (Applied applied : o.applications(ack.sessionId(), ack.sequence())) {
                if (!isRejection(applied.result()) && !applied.result().equals(ack.result())) {
                    return "client " + ack.client() + " was told " + ack.result() + " for command "
                            + ack.sequence() + " of session " + ack.sessionId() + ", which gave "
                            + applied.result() + " at entry " + applied.index();
                }
            }
        }
        return null;
    }

    /** A result that says the command was not looked at: it tells nothing about its effect. */
    private static boolean isRejection(CommandResult result) {
        return result instanceof Failed failed && (failed.failure() == Failure.SEQUENCE_TOO_OLD
                || failed.failure() == Failure.NO_SUCH_SESSION || failed.failure() == Failure.NO_SPACE);
    }
}
