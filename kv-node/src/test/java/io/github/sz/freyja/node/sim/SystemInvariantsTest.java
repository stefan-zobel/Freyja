package io.github.sz.freyja.node.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.common.kv.CommandResult;
import io.github.sz.freyja.common.kv.CommandResult.ConditionalApplied;
import io.github.sz.freyja.common.kv.CommandResult.Failed;
import io.github.sz.freyja.common.kv.CommandResult.Failure;
import io.github.sz.freyja.common.kv.CommandResult.Written;
import io.github.sz.freyja.node.sim.SystemObservation.Acknowledged;
import io.github.sz.freyja.node.sim.SystemObservation.Applied;
import io.github.sz.freyja.node.sim.SystemObservation.Digest;

/**
 * Every invariant of the whole system accepts a correct history and rejects a broken one
 * (docs/spec/verification.md, section 2.6): an invariant that cannot fail proves nothing.
 */
class SystemInvariantsTest {

    private static final String SOURCE =
            "kv-node/src/test/java/io/github/sz/freyja/node/sim/SystemInvariants.java";

    private final SystemObservation o = new SystemObservation();

    private static Applied applied(int node, long index, long hash, long session, long sequence,
                                   CommandResult result) {
        return new Applied(node, index, 1, hash, session, sequence, result);
    }

    /** One step: what happens in {@code step} is what the invariants judge next. */
    private void step(Runnable step) {
        o.beginStep();
        step.run();
    }

    private void holds() {
        for (SystemInvariants.Invariant invariant : SystemInvariants.ALL) {
            assertNull(invariant.predicate().apply(o), invariant.name());
        }
        SystemInvariants.checkAll(o);
    }

    private void violates(String name) {
        for (SystemInvariants.Invariant invariant : SystemInvariants.ALL) {
            if (invariant.name().equals(name)) {
                assertNotNull(invariant.predicate().apply(o), name + " must reject this history");
                AssertionError error = assertThrows(AssertionError.class, () -> SystemInvariants.checkAll(o));
                assertTrue(error.getMessage().contains(" violated"), error.getMessage());
                return;
            }
        }
        throw new AssertionError("no invariant " + name);
    }

    @Test
    void aCorrectHistoryIsAccepted() {
        step(() -> {
            o.applied(applied(0, 1, 11, 0, 0, null));
            o.applied(applied(0, 2, 22, 1, 1, new Written()));
            o.digest(new Digest(0, 2, 777));
        });
        holds();
        step(() -> o.acknowledged(new Acknowledged(5, 1, 1, 2, 22, new Written())));
        holds();
        step(() -> {
            o.applied(applied(1, 1, 11, 0, 0, null));
            o.applied(applied(1, 2, 22, 1, 1, new Written()));
            o.digest(new Digest(1, 2, 777));
            // The same command a second time in the log: answered from the record.
            o.applied(applied(1, 3, 22, 1, 1, new Written()));
            // An older command arrives after the session has moved on.
            o.applied(applied(1, 4, 33, 1, 2, new ConditionalApplied(false)));
            o.applied(applied(1, 5, 22, 1, 1, new Failed(Failure.SEQUENCE_TOO_OLD)));
        });
        holds();
        step(() -> {
            // Rejected for lack of space, applied later, and the session ends.
            o.applied(applied(0, 6, 44, 1, 3, new Failed(Failure.NO_SPACE)));
            o.applied(applied(0, 7, 44, 1, 3, new Written()));
            o.applied(applied(0, 8, 44, 1, 3, new Failed(Failure.NO_SUCH_SESSION)));
            o.applied(applied(0, 9, 44, 1, 3, new Failed(Failure.NO_SUCH_SESSION)));
        });
        holds();
        step(() -> o.acknowledged(new Acknowledged(5, 1, 3, 8, 44, new Failed(Failure.NO_SUCH_SESSION))));
        holds();
        assertEquals(9, o.highestApplied());
    }

    @Test
    void anotherEntryAtTheSameIndexViolatesStateMachineEquality() {
        step(() -> o.applied(applied(0, 1, 11, 0, 0, null)));
        holds();
        step(() -> o.applied(applied(1, 1, 12, 0, 0, null)));
        violates("StateMachineEquality");
    }

    @Test
    void anotherResultForTheSameEntryViolatesStateMachineEquality() {
        step(() -> o.applied(applied(0, 1, 11, 1, 1, new ConditionalApplied(true))));
        holds();
        step(() -> o.applied(applied(1, 1, 11, 1, 1, new ConditionalApplied(false))));
        violates("StateMachineEquality");
    }

    @Test
    void anotherStateAfterTheSameEntriesViolatesStateMachineEquality() {
        step(() -> o.digest(new Digest(0, 7, 100)));
        holds();
        step(() -> o.digest(new Digest(2, 7, 101)));
        violates("StateMachineEquality");
    }

    @Test
    void anAnswerForAnEntryNobodyAppliedViolatesNoAcknowledgedWriteLost() {
        step(() -> o.acknowledged(new Acknowledged(5, 1, 1, 2, 22, new Written())));
        violates("NoAcknowledgedWriteLost");
    }

    @Test
    void anAnswerThatDiffersFromWhatWasAppliedViolatesNoAcknowledgedWriteLost() {
        step(() -> o.applied(applied(0, 2, 22, 1, 1, new ConditionalApplied(false))));
        step(() -> o.acknowledged(new Acknowledged(5, 1, 1, 2, 22, new ConditionalApplied(true))));
        violates("NoAcknowledgedWriteLost");
    }

    @Test
    void anotherEntryAtAnAcknowledgedIndexViolatesNoAcknowledgedWriteLost() {
        step(() -> o.applied(applied(0, 2, 22, 1, 1, new Written())));
        step(() -> o.acknowledged(new Acknowledged(5, 1, 1, 2, 22, new Written())));
        holds();
        // A node whose log lost the acknowledged entry applies something else in its place.
        step(() -> o.applied(applied(1, 2, 99, 2, 1, new Written())));
        assertNotNull(SystemInvariants.noAcknowledgedWriteLost(o));
        violates("NoAcknowledgedWriteLost");
    }

    @Test
    void aCommandThatTakesEffectTwiceViolatesExactlyOnce() {
        step(() -> o.applied(applied(0, 2, 22, 1, 1, new ConditionalApplied(true))));
        holds();
        // The same command again, and this time its condition is looked at anew.
        step(() -> o.applied(applied(0, 5, 22, 1, 1, new ConditionalApplied(false))));
        violates("ExactlyOnce");
    }

    @Test
    void aCommandOfASessionThatWasGoneViolatesExactlyOnce() {
        step(() -> o.applied(applied(0, 2, 22, 1, 1, new Failed(Failure.NO_SUCH_SESSION))));
        holds();
        step(() -> o.applied(applied(0, 5, 22, 1, 1, new Written())));
        violates("ExactlyOnce");
    }

    @Test
    void aClientToldAnotherResultThanTheOneOfTheEffectViolatesExactlyOnce() {
        step(() -> {
            o.applied(applied(0, 2, 22, 1, 1, new ConditionalApplied(true)));
            o.applied(applied(0, 3, 22, 1, 1, new ConditionalApplied(true)));
        });
        holds();
        step(() -> o.acknowledged(new Acknowledged(5, 1, 1, 3, 22, new ConditionalApplied(false))));
        assertNotNull(SystemInvariants.exactlyOnce(o));
    }

    private static Path repositoryRoot() {
        Path here = Path.of("").toAbsolutePath();
        return Files.exists(here.resolve("docs")) ? here : here.getParent();
    }

    @Test
    void theChecksumOfTheProtectedFileIsOnRecord() throws Exception {
        Path root = repositoryRoot();
        Path source = root;
        for (String part : SOURCE.split("/")) {
            source = source.resolve(part);
        }
        // Line ends are normalized, so that the checksum is the same on every platform.
        String text = Files.readString(source, StandardCharsets.UTF_8).replace("\r\n", "\n");
        String actual = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));

        String recorded = null;
        for (String line : Files.readAllLines(root.resolve("docs").resolve("spec").resolve("invariants.sha256"))) {
            if (!line.startsWith("#") && line.endsWith(SOURCE)) {
                recorded = line.substring(0, line.indexOf(' '));
            }
        }
        assertNotNull(recorded, "no checksum on record for " + SOURCE);
        assertEquals(recorded, actual, "SystemInvariants.java was changed. This file is protected: it is "
                + "changed only on the explicit instruction of the project owner. If this change is "
                + "one, record the new checksum in docs/spec/invariants.sha256.");
    }
}
