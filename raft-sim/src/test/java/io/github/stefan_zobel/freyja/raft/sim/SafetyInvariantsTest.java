package io.github.stefan_zobel.freyja.raft.sim;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.raft.EntryType;
import io.github.stefan_zobel.freyja.raft.HardState;
import io.github.stefan_zobel.freyja.raft.LogEntry;
import io.github.stefan_zobel.freyja.raft.Outbound;
import io.github.stefan_zobel.freyja.raft.RaftMessage.AppendEntries;
import io.github.stefan_zobel.freyja.raft.RaftMessage.AppendResponse;
import io.github.stefan_zobel.freyja.raft.RaftMessage.Heartbeat;
import io.github.stefan_zobel.freyja.raft.RaftMessage.RequestVote;
import io.github.stefan_zobel.freyja.raft.RaftMessage.VoteResponse;

/**
 * The protected invariants (docs/spec/verification.md, section 2.6): the checksum of the file
 * is on record, and every invariant rejects a deliberately broken history. An invariant that
 * cannot fail proves nothing.
 */
class SafetyInvariantsTest {

    private static final String SOURCE =
            "raft-sim/src/main/java/io/github/stefan_zobel/freyja/raft/sim/SafetyInvariants.java";

    private static LogEntry entry(long index, long term, int content) {
        return new LogEntry(index, term, EntryType.COMMAND, new byte[] {(byte) content});
    }

    private static HardState hard(long term, int votedFor) {
        return new HardState(term, votedFor, false);
    }

    /** A history without any violation: two nodes, one leader, one applied entry. */
    private static Observation clean() {
        Observation o = new Observation(3);
        LogEntry first = entry(1, 1, 7);
        o.elected(0, 1);
        o.appended(0, first);
        o.appended(1, first);
        o.released(0, new Outbound(1, new AppendEntries(1, 0, 0, List.of(first), 0)), hard(1, 0), 0);
        o.released(1, new Outbound(0, new AppendResponse(1, true, 1, 0, 0)), hard(1, 0), 1);
        o.released(1, new Outbound(0, new VoteResponse(1, false, true)), hard(1, 0), 0);
        o.released(0, new Outbound(1, new RequestVote(1, 0, 0, false)), hard(1, 0), 0);
        o.released(0, new Outbound(1, new Heartbeat(1, 0, 1)), hard(1, 0), 0);
        // Messages that promise nothing may leave before anything is durable.
        o.released(2, new Outbound(0, new RequestVote(9, 0, 0, true)), hard(0, -1), 0);
        o.released(2, new Outbound(0, new VoteResponse(9, true, true)), hard(0, -1), 0);
        o.released(2, new Outbound(0, new AppendResponse(9, false, 4, 0, 0)), hard(0, -1), 0);
        // A node that has moved on to a later term has outlived its old promises.
        o.released(1, new Outbound(0, new AppendResponse(1, true, 5, 0, 0)), hard(2, -1), 0);
        o.applied(0, first);
        o.applied(1, first);
        o.sawCommitted(1, 1);
        return o;
    }

    /** For every invariant: histories that break it. */
    private static Map<String, List<Supplier<Observation>>> broken() {
        Map<String, List<Supplier<Observation>>> broken = new LinkedHashMap<>();
        broken.put("ElectionSafety", List.of(() -> {
            Observation o = clean();
            o.beginStep();
            o.elected(1, 1); // a second leader of term 1
            return o;
        }));
        broken.put("LogMatching", List.of(() -> {
            Observation o = clean();
            o.beginStep();
            o.appended(2, entry(1, 1, 8)); // same index and term, other content
            return o;
        }, () -> {
            Observation o = new Observation(2);
            o.appended(0, entry(1, 1, 1));
            o.appended(0, entry(2, 3, 2));
            o.appended(1, entry(1, 2, 9));
            o.beginStep();
            o.appended(1, entry(2, 3, 2)); // equal at index 2, different before it
            return o;
        }));
        broken.put("LeaderCompleteness", List.of(() -> {
            Observation o = clean();
            o.beginStep();
            o.elected(2, 2); // node 2 has an empty log, entry 1 was committed in term 1
            return o;
        }, () -> {
            Observation o = clean();
            o.beginStep();
            o.appended(2, entry(1, 2, 5));
            o.elected(2, 3); // holds another entry where one was committed
            return o;
        }));
        broken.put("StateMachineSafety", List.of(() -> {
            Observation o = clean();
            o.beginStep();
            o.applied(2, entry(1, 2, 5));
            return o;
        }, () -> {
            Observation o = clean();
            o.beginStep();
            o.applied(2, entry(3, 1, 5)); // beyond everything applied so far
            return o;
        }));
        broken.put("DurabilityBeforeVisibility", List.of(() -> {
            Observation o = clean();
            o.beginStep(); // a vote whose term is not durable
            o.released(1, new Outbound(0, new VoteResponse(3, false, true)), hard(2, -1), 0);
            return o;
        }, () -> {
            Observation o = clean();
            o.beginStep(); // a vote for node 0 while the durable vote is for node 2
            o.released(1, new Outbound(0, new VoteResponse(3, false, true)), hard(3, 2), 0);
            return o;
        }, () -> {
            Observation o = clean();
            o.beginStep(); // an acknowledgment beyond the durable log
            o.released(1, new Outbound(0, new AppendResponse(1, true, 5, 0, 0)), hard(1, 0), 4);
            return o;
        }, () -> {
            Observation o = clean();
            o.beginStep(); // a candidate that asks before its own vote is durable
            o.released(2, new Outbound(0, new RequestVote(4, 0, 0, false)), hard(4, -1), 0);
            return o;
        }, () -> {
            Observation o = clean();
            o.beginStep(); // a leader whose term is not durable
            o.released(2, new Outbound(0, new Heartbeat(4, 0, 1)), hard(3, 2), 0);
            return o;
        }));
        return broken;
    }

    @Test
    void everyInvariantHoldsOnACleanHistory() {
        Observation o = clean();
        for (SafetyInvariants.Invariant invariant : SafetyInvariants.ALL) {
            assertNull(invariant.predicate().apply(o), invariant.name());
        }
        SafetyInvariants.checkAll(o);
        assertNull(SafetyInvariants.logMatchingComplete(o));
        // A leader of the same term in which an entry was committed need not hold it.
        Observation same = clean();
        same.beginStep();
        same.appended(2, entry(1, 1, 7));
        assertNull(SafetyInvariants.leaderCompleteness(same));
    }

    @Test
    void everyInvariantRejectsABrokenHistory() {
        Map<String, List<Supplier<Observation>>> broken = broken();
        assertEquals(SafetyInvariants.byName().keySet(), broken.keySet(),
                "every invariant needs a negative test");
        for (SafetyInvariants.Invariant invariant : SafetyInvariants.ALL) {
            for (Supplier<Observation> history : broken.get(invariant.name())) {
                Observation o = history.get();
                String violation = invariant.predicate().apply(o);
                assertNotNull(violation, invariant.name() + " accepted a broken history");
                AssertionError error = assertThrows(AssertionError.class, () -> SafetyInvariants.checkAll(o));
                assertTrue(error.getMessage().startsWith(invariant.name() + " violated"), error.getMessage());
            }
        }
    }

    @Test
    void theCompleteLogMatchingCheckRejectsDivergedPrefixes() {
        Observation o = new Observation(2);
        o.resetLog(0, List.of(entry(1, 1, 1), entry(2, 2, 2), entry(3, 3, 3)));
        o.resetLog(1, List.of(entry(1, 1, 9), entry(2, 2, 2)));
        assertNotNull(SafetyInvariants.logMatchingComplete(o));
        o.resetLog(1, List.of(entry(1, 1, 1), entry(2, 2, 2), entry(3, 4, 4)));
        assertNull(SafetyInvariants.logMatchingComplete(o), "logs may differ after their common part");
    }

    /** The module directory is the working directory of the tests. */
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
        assertEquals(recorded, actual, "SafetyInvariants.java was changed. This file is protected: it is "
                + "changed only on the explicit instruction of the project owner. If this change is "
                + "one, record the new checksum in docs/spec/invariants.sha256.");
    }
}
