package io.github.stefan_zobel.freyja.tools;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.lsm.fs.MemoryFileSystem;
import io.github.stefan_zobel.freyja.node.Node;
import io.github.stefan_zobel.freyja.node.store.HardStateStore;
import io.github.stefan_zobel.freyja.node.store.NodeIdentity;
import io.github.stefan_zobel.freyja.raft.HardState;

/** The dump tool for the identity and the hard state of a node. */
class NodeStateDumpTest {

    private static final Path DIR = Path.of("data");

    private final MemoryFileSystem fs = new MemoryFileSystem();
    private final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();

    private int run(String... args) {
        outBytes.reset();
        return NodeStateDump.run(args, fs, new PrintStream(outBytes, true, StandardCharsets.UTF_8),
                new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8));
    }

    private String out() {
        return outBytes.toString(StandardCharsets.UTF_8);
    }

    @Test
    void theStateIsPrintedAndNothingIsChanged() throws IOException {
        Node.initialize(fs, DIR, new NodeIdentity(0xABCDEFL, 4));
        HardStateStore.open(fs, DIR).put(new HardState(9, 2, true, 3, 120, HardState.UNKNOWN_TERM));
        Path a = DIR.resolve(HardStateStore.FILE_A);
        byte[] before = fs.peek(a, 0, (int) fs.size(a));

        assertEquals(0, run(DIR.toString()), out());
        String text = out();
        assertTrue(text.contains("cluster abcdef, node 4"), text);
        assertTrue(text.contains("counter 2, term 9, vote for node 2, log epoch 3"), text);
        assertTrue(text.contains("LOG-RECOVERING (lost log ended at index 120, term unknown)"), text);
        assertTrue(text.contains("the node can start"), text);

        // One damaged file: still usable, and the tool does not repair it.
        byte[] one = fs.peek(a, 12, 1);
        one[0] ^= 1;
        fs.overwrite(a, 12, one);
        byte[] damaged = fs.peek(a, 0, (int) fs.size(a));
        assertEquals(0, run(DIR.toString()));
        assertTrue(out().contains(HardStateStore.FILE_A + ":  missing or damaged"), out());
        assertTrue(out().contains(HardStateStore.FILE_B + " counts; the other file is written again"), out());
        assertArrayEquals(damaged, fs.peek(a, 0, (int) fs.size(a)));
        assertEquals(before.length, damaged.length);
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void aLostIdentityIsReported() throws IOException {
        Node.initialize(fs, DIR, new NodeIdentity(1, 1));
        fs.delete(DIR.resolve(HardStateStore.FILE_A));
        fs.delete(DIR.resolve(HardStateStore.FILE_B));
        assertEquals(1, run(DIR.toString()));
        assertTrue(out().contains("hard state:   LOST"), out());
        fs.delete(DIR.resolve(NodeIdentity.FILE_NAME));
        assertEquals(1, run(DIR.toString()));
        assertTrue(out().contains("identity:     LOST"), out());
        assertEquals(2, run());
        assertEquals(2, run("--x"));
    }
}
