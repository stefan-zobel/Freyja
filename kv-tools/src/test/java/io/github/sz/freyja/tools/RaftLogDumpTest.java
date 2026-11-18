package io.github.sz.freyja.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.lsm.fs.MemoryFileSystem;
import io.github.sz.freyja.node.raftlog.RaftLogStore;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.ConfigChange;
import io.github.sz.freyja.raft.EntryType;
import io.github.sz.freyja.raft.LogEntry;

/** The dump tool for segments of the Raft log. */
class RaftLogDumpTest {

    private static final Path DIR = Path.of("data", "raft");
    private static final ClusterConfig THREE = ClusterConfig.ofVoters(0, 1, 2);

    private final MemoryFileSystem fs = new MemoryFileSystem();
    private final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();

    private int run(String... args) {
        outBytes.reset();
        errBytes.reset();
        return RaftLogDump.run(args, fs, new PrintStream(outBytes, true, StandardCharsets.UTF_8),
                new PrintStream(errBytes, true, StandardCharsets.UTF_8));
    }

    private String out() {
        return outBytes.toString(StandardCharsets.UTF_8);
    }

    private static List<LogEntry> batch(long from, int count, long term) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            entries.add(new LogEntry(from + i, term, EntryType.COMMAND, new byte[100]));
        }
        return entries;
    }

    /** Two segments: the first is full and sealed, the second holds a truncation and a configuration. */
    private void build() throws IOException {
        try (RaftLogStore store = RaftLogStore.open(fs, DIR, 600, 0, THREE)) {
            store.append(batch(1, 3, 1));
            store.sync();
            store.append(batch(4, 3, 1));
            store.sync();
            store.truncateSuffix(5);
            store.append(List.of(new LogEntry(5, 2, EntryType.CONFIG,
                    THREE.with(new ConfigChange.AddLearner(7)).encode())));
            store.sync();
        }
    }

    @Test
    void aSegmentIsPrintedRecordByRecord() throws IOException {
        build();
        assertEquals(0, run("--sealed", DIR.resolve("0000000001.rlog").toString()), out());
        String first = out();
        assertTrue(first.contains("header at 14: first index 1, follows segment 0"), first);
        assertTrue(first.contains("configuration voters [0, 1, 2]"), first);
        assertTrue(first.contains("records:      2 with 6 entries (1..6)"), first);
        assertTrue(first.contains("end:          clean"), first);

        assertEquals(0, run("--entries", DIR.resolve("0000000002.rlog").toString()), out());
        String second = out();
        assertTrue(second.contains("first index 7, follows segment 1"), second);
        assertTrue(second.contains(": from index 5"), second);
        assertTrue(second.contains("  5 term 2 CONFIG"), second);
        assertTrue(second.contains("learners [7]"), second);
        assertTrue(second.contains("truncations:  1"), second);
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void damageATornTailAndWrongUsageAreToldApart() throws IOException {
        build();
        Path first = DIR.resolve("0000000001.rlog");
        byte[] one = fs.peek(first, 100, 1);
        one[0] ^= 4;
        fs.overwrite(first, 100, one);
        assertEquals(1, run("--sealed", first.toString()));
        assertTrue(errBytes.toString(StandardCharsets.UTF_8).contains("corruption"));

        // A segment whose end was not synced.
        try (RaftLogStore store = RaftLogStore.open(fs, DIR, 1 << 20, 0, THREE)) {
            store.quarantineDamaged();
            store.append(batch(store.lastIndex() + 1, 2, 3));
            fs.crash(new java.util.Random(1));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        Path last = fs.list(DIR).stream().filter(p -> p.getFileName().toString().endsWith(".rlog"))
                .sorted().toList().getLast();
        assertEquals(0, run(last.toString()), out() + errBytes);

        assertEquals(2, run());
        assertEquals(2, run("--bogus", last.toString()));
        assertEquals(2, run(DIR.resolve("segment.rlog").toString()), "no file number in the name");
        assertEquals(1, run(DIR.resolve("0000000099.rlog").toString()), "no such file");
    }
}
