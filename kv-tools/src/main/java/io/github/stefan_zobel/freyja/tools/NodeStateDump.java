package io.github.stefan_zobel.freyja.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import io.github.stefan_zobel.freyja.lsm.fs.RealFileSystem;
import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;
import io.github.stefan_zobel.freyja.node.store.HardStateStore;
import io.github.stefan_zobel.freyja.node.store.LostIdentityException;
import io.github.stefan_zobel.freyja.node.store.NodeIdentity;
import io.github.stefan_zobel.freyja.raft.HardState;

/**
 * Prints the identity and the hard state of a node from its data directory
 * (docs/spec/node.md, sections 2 and 3.2). It reads the files and changes nothing; in
 * particular it does not repair a hard-state file from the other one, as a starting node does.
 *
 * <pre>
 * usage: NodeStateDump DATA-DIRECTORY
 * </pre>
 *
 * <p>Exit code 0: the node could start with what is there. 1: the identity is lost (no valid
 * identity file, or no valid hard-state file), or an I/O error. 2: wrong usage.
 */
public final class NodeStateDump {

    private NodeStateDump() {
    }

    public static void main(String[] args) {
        System.exit(run(args, new RealFileSystem(), System.out, System.err));
    }

    public static int run(String[] args, StorageFileSystem fs, PrintStream out, PrintStream err) {
        if (args.length != 1 || args[0].startsWith("--")) {
            err.println("usage: NodeStateDump DATA-DIRECTORY");
            return 2;
        }
        Path directory = Path.of(args[0]);
        try {
            boolean usable = true;
            out.println("directory:    " + directory);
            try {
                NodeIdentity identity = NodeIdentity.read(fs, directory);
                out.println("identity:     cluster " + Long.toHexString(identity.clusterId()) + ", node "
                        + identity.nodeId());
            } catch (LostIdentityException e) {
                out.println("identity:     LOST (" + NodeIdentity.FILE_NAME + " is missing or damaged)");
                usable = false;
            }
            HardStateStore.Stored a = HardStateStore.inspect(fs, directory, HardStateStore.FILE_A);
            HardStateStore.Stored b = HardStateStore.inspect(fs, directory, HardStateStore.FILE_B);
            out.println(HardStateStore.FILE_A + ":  " + describe(a));
            out.println(HardStateStore.FILE_B + ":  " + describe(b));
            if (a == null && b == null) {
                out.println("hard state:   LOST (no file holds a valid record)");
                usable = false;
            } else {
                boolean useA = b == null || (a != null && a.counter() >= b.counter());
                out.println("hard state:   " + (useA ? HardStateStore.FILE_A : HardStateStore.FILE_B) + " counts"
                        + (a == null || b == null || a.counter() != b.counter()
                                ? "; the other file is written again at the next start" : ""));
            }
            out.println("result:       " + (usable ? "the node can start" : "the identity is lost; replace the member"));
            return usable ? 0 : 1;
        } catch (IOException e) {
            err.println("error: " + e.getMessage());
            return 1;
        }
    }

    private static String describe(HardStateStore.Stored stored) {
        if (stored == null) {
            return "missing or damaged";
        }
        HardState state = stored.state();
        StringBuilder text = new StringBuilder("counter " + stored.counter() + ", term " + state.term() + ", vote "
                + (state.votedFor() == HardState.NO_VOTE ? "none" : "for node " + state.votedFor())
                + ", log epoch " + state.logEpoch());
        if (state.logRecovering()) {
            text.append(", LOG-RECOVERING (lost log ended at index ").append(state.lostLogIndex())
                    .append(state.lostLogTerm() == HardState.UNKNOWN_TERM ? ", term unknown)"
                            : ", term " + state.lostLogTerm() + ")");
        }
        return text.toString();
    }
}
