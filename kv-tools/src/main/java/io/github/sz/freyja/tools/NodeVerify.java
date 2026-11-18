package io.github.sz.freyja.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import io.github.sz.freyja.lsm.fs.RealFileSystem;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.node.Node;
import io.github.sz.freyja.node.sm.StateMachineStore;
import io.github.sz.freyja.node.store.GenerationPointer;

/**
 * Verifies the data directory of a node that is not running (docs/spec/operations.md,
 * section 4): its small files, every segment of its Raft log, and the storage engine of its
 * state machine. It reads and changes nothing.
 *
 * <pre>
 * usage: NodeVerify DATA-DIRECTORY
 * </pre>
 *
 * <p>Exit code 0: nothing is damaged. 1: something is damaged or lost, or an I/O error; the
 * output says what, and what the node will do about it at its next start. 2: wrong usage.
 */
public final class NodeVerify {

    private NodeVerify() {
    }

    public static void main(String[] args) {
        System.exit(run(args, new RealFileSystem(), System.out, System.err));
    }

    public static int run(String[] args, StorageFileSystem fs, PrintStream out, PrintStream err) {
        if (args.length != 1 || args[0].startsWith("--")) {
            err.println("usage: NodeVerify DATA-DIRECTORY");
            return 2;
        }
        Path directory = Path.of(args[0]);
        boolean intact = NodeStateDump.run(new String[] {args[0]}, fs, out, err) == 0;
        try {
            // The Raft log: every segment but the newest was sealed by its successor.
            Path raft = directory.resolve(Node.RAFT_DIRECTORY);
            List<Path> segments = new ArrayList<>();
            if (fs.exists(raft)) {
                for (Path path : fs.list(raft)) {
                    if (path.getFileName().toString().endsWith(".rlog")) {
                        segments.add(path);
                    }
                }
            }
            int damaged = 0;
            for (int i = 0; i < segments.size(); i++) {
                ByteArrayOutputStream detail = new ByteArrayOutputStream();
                PrintStream quiet = new PrintStream(detail, true, StandardCharsets.UTF_8);
                String[] arguments = i < segments.size() - 1
                        ? new String[] {"--sealed", segments.get(i).toString()}
                        : new String[] {segments.get(i).toString()};
                if (RaftLogDump.run(arguments, fs, quiet, quiet) != 0) {
                    damaged++;
                    out.println("raft log:     " + segments.get(i).getFileName() + " is DAMAGED");
                    detail.toString(StandardCharsets.UTF_8).lines()
                            .filter(line -> line.startsWith("error") || line.contains("damage"))
                            .forEach(line -> out.println("              " + line));
                }
            }
            out.println("raft log:     " + segments.size() + " segments, " + (damaged == 0 ? "none damaged"
                    : damaged + " damaged; the node will refill its log from the leader"));
            intact &= damaged == 0;

            // The state machine: the generation that counts.
            Path machines = directory.resolve(StateMachineStore.DIRECTORY);
            GenerationPointer pointer = fs.exists(machines) ? GenerationPointer.read(fs, machines) : null;
            if (pointer == null) {
                out.println("state machine: no generation is named; the node will rebuild it");
                intact = false;
            } else {
                Path generation = machines.resolve(GenerationPointer.directoryName(pointer.generation()));
                ByteArrayOutputStream detail = new ByteArrayOutputStream();
                PrintStream quiet = new PrintStream(detail, true, StandardCharsets.UTF_8);
                boolean sound = fs.exists(generation)
                        && DbVerify.run(new String[] {generation.toString()}, fs, quiet, quiet) == 0;
                out.println("state machine: generation " + pointer.generation() + (sound ? " is intact"
                        : " is DAMAGED; the node will rebuild it"));
                if (!sound) {
                    detail.toString(StandardCharsets.UTF_8).lines().limit(10)
                            .forEach(line -> out.println("              " + line));
                }
                intact &= sound;
            }
        } catch (IOException e) {
            err.println("error: " + e.getMessage());
            return 1;
        }
        out.println("verdict:      " + (intact ? "nothing is damaged" : "DAMAGE, see above"));
        return intact ? 0 : 1;
    }
}
