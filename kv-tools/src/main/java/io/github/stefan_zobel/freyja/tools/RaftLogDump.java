package io.github.stefan_zobel.freyja.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.List;

import io.github.stefan_zobel.freyja.lsm.fs.ReadableFile;
import io.github.stefan_zobel.freyja.lsm.fs.RealFileSystem;
import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;
import io.github.stefan_zobel.freyja.lsm.log.FileKind;
import io.github.stefan_zobel.freyja.lsm.log.LogReader;
import io.github.stefan_zobel.freyja.node.raftlog.RaftLogStore;
import io.github.stefan_zobel.freyja.raft.ClusterConfig;
import io.github.stefan_zobel.freyja.raft.EntryType;
import io.github.stefan_zobel.freyja.raft.LogEntry;

/**
 * Prints the content of one segment of the Raft log (docs/spec/node.md, section 3.1). It
 * reads the file and changes nothing.
 *
 * <pre>
 * usage: RaftLogDump [--file-number N] [--sealed] [--entries] FILE
 * </pre>
 *
 * <ul>
 *   <li>{@code --sealed}: a later segment exists, so the file cannot end in a torn tail.
 *       Without it the file is read as the last segment. A sealed segment that follows a
 *       crash is valid only up to the length its successor records; the tool does not know
 *       that length and reads the whole file.</li>
 *   <li>{@code --entries}: one line per entry instead of one per record.</li>
 * </ul>
 *
 * <p>Exit code 0: the file was read to its end, possibly a torn tail. 1: damage or an I/O
 * error. 2: wrong usage.
 */
public final class RaftLogDump {

    private RaftLogDump() {
    }

    public static void main(String[] args) {
        System.exit(run(args, new RealFileSystem(), System.out, System.err));
    }

    public static int run(String[] args, StorageFileSystem fs, PrintStream out, PrintStream err) {
        Path file = null;
        Long fileNumber = null;
        boolean sealed = false;
        boolean entries = false;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("--entries")) {
                entries = true;
            } else if (arg.equals("--sealed")) {
                sealed = true;
            } else if (arg.equals("--file-number") && i + 1 < args.length) {
                try {
                    fileNumber = Long.parseLong(args[++i]);
                } catch (NumberFormatException e) {
                    return usage(err, "not a file number: " + args[i]);
                }
            } else if (arg.startsWith("--") || file != null) {
                return usage(err, "unexpected argument: " + arg);
            } else {
                file = Path.of(arg);
            }
        }
        if (file == null) {
            return usage(err, "no file given");
        }
        if (fileNumber == null) {
            fileNumber = SstDump.fileNumberFromName(file);
            if (fileNumber == null) {
                return usage(err, "cannot derive the file number from the name "
                        + file.getFileName() + "; pass --file-number");
            }
        }
        try {
            dump(fs, file, fileNumber, sealed, entries, out);
            return 0;
        } catch (IOException e) {
            err.println("error: " + e.getMessage());
            return 1;
        }
    }

    private static int usage(PrintStream err, String problem) {
        err.println("error: " + problem);
        err.println("usage: RaftLogDump [--file-number N] [--sealed] [--entries] FILE");
        return 2;
    }

    public static void dump(StorageFileSystem fs, Path file, long fileNumber, boolean sealed,
                            boolean entries, PrintStream out) throws IOException {
        String name = file.toString();
        try (ReadableFile readable = fs.open(file)) {
            out.println("file:         " + file);
            out.println("file number:  " + fileNumber);
            out.println("file size:    " + readable.size());
            out.println("treated as:   " + (sealed ? "sealed" : "last segment"));

            long[] counts = new long[4]; // entries records, entries, truncations, sync points
            long[] range = {-1, -1};
            boolean[] headerSeen = {false};
            LogReader.Result result = LogReader.read(readable, name, fileNumber, FileKind.RAFT_LOG, sealed,
                    new LogReader.Listener() {
                        @Override
                        public void record(long offset, byte[] payload) throws IOException {
                            int type = payload.length == 0 ? -1 : payload[0] & 0xFF;
                            if (!headerSeen[0]) {
                                headerSeen[0] = true;
                                RaftLogStore.SegmentHeader header = RaftLogStore.decodeHeader(payload, name);
                                out.println("header at " + offset + ": first index " + header.firstIndex()
                                        + ", follows segment " + header.previousNumber() + " (valid length "
                                        + header.previousValidLength() + "), configuration "
                                        + describe(header.config()));
                                return;
                            }
                            if (type == RaftLogStore.RECORD_ENTRIES) {
                                List<LogEntry> batch = RaftLogStore.decodeEntries(payload, name, offset);
                                counts[0]++;
                                counts[1] += batch.size();
                                if (!batch.isEmpty()) {
                                    if (range[0] == -1) {
                                        range[0] = batch.getFirst().index();
                                    }
                                    range[1] = batch.getLast().index();
                                    out.println("entries at " + offset + ": " + batch.getFirst().index() + ".."
                                            + batch.getLast().index() + ", " + payload.length + " bytes");
                                }
                                if (entries) {
                                    for (LogEntry entry : batch) {
                                        out.println("  " + entry.index() + " term " + entry.term() + " "
                                                + entry.type() + " " + entry.payload().length + " bytes"
                                                + (entry.type() == EntryType.CONFIG
                                                        ? " " + describe(ClusterConfig.decode(entry.payload())) : ""));
                                    }
                                }
                            } else if (type == RaftLogStore.RECORD_TRUNCATE) {
                                counts[2]++;
                                out.println("truncation at " + offset + ": from index "
                                        + RaftLogStore.decodeTruncate(payload, name, offset));
                            } else {
                                out.println("record at " + offset + " of the unknown type " + type + ", "
                                        + payload.length + " bytes");
                            }
                        }

                        @Override
                        public void syncPoint(long offset, long syncedLength) {
                            counts[3]++;
                            out.println("sync point at " + offset + ": " + syncedLength + " bytes synced");
                        }
                    });

            out.println("records:      " + counts[0] + " with " + counts[1] + " entries"
                    + (range[0] == -1 ? "" : " (" + range[0] + ".." + range[1] + ")"));
            out.println("truncations:  " + counts[2]);
            out.println("sync points:  " + counts[3]);
            if (!headerSeen[0]) {
                out.println("header:       none (the segment was never used)");
            }
            if (result.tornTail()) {
                out.println("end:          torn tail at " + result.failureOffset() + " ("
                        + result.failure() + "); valid length " + result.validLength());
            } else {
                out.println("end:          clean");
            }
        }
    }

    private static String describe(ClusterConfig config) {
        return "voters " + config.voters() + (config.learners().isEmpty() ? "" : ", learners " + config.learners());
    }
}
