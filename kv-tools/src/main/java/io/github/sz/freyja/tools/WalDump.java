package io.github.sz.freyja.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.HexFormat;

import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.lsm.WriteBatch;
import io.github.sz.freyja.lsm.fs.ReadableFile;
import io.github.sz.freyja.lsm.fs.RealFileSystem;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.lsm.log.LogReader;

/**
 * Offline tool that prints the records and sync points of a write-ahead log and reports how
 * the file ends. It works for every file in the WAL record format; the contents of records
 * are decoded only if they are write batches.
 *
 * <pre>
 * usage: WalDump [--file-number N] [--sealed] [--entries] FILE
 * </pre>
 *
 * <ul>
 *   <li>{@code --file-number}: checksums are bound to the file number. By default it is
 *       taken from the file name ({@code 000042.wal} is file 42).</li>
 *   <li>{@code --sealed}: treat the file as sealed, that is, as followed by a newer log. In
 *       a sealed file every invalid record is damage. Without the flag the file is treated
 *       as the last log, which may end in a torn tail.</li>
 *   <li>{@code --entries}: print every put and delete of every batch.</li>
 * </ul>
 *
 * <p>The tool never changes the file.
 */
public final class WalDump {

    private static final HexFormat HEX = HexFormat.of();
    private static final int MAX_VALUE_BYTES_SHOWN = 32;

    private WalDump() {
    }

    public static void main(String[] args) {
        System.exit(run(args, new RealFileSystem(), System.out, System.err));
    }

    /**
     * Runs the tool.
     *
     * @return the process exit code: 0 if the file is clean or ends in a torn tail, 1 if it
     *         could not be read or is damaged, 2 on a usage error
     */
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
        err.println("usage: WalDump [--file-number N] [--sealed] [--entries] FILE");
        return 2;
    }

    /**
     * Prints the description of one log.
     *
     * @param sealed  whether to classify the file as sealed
     * @param entries whether to print the entries of every batch
     */
    public static void dump(StorageFileSystem fs, Path file, long fileNumber, boolean sealed,
                            boolean entries, PrintStream out) throws IOException {
        try (ReadableFile readable = fs.open(file)) {
            out.println("file:         " + file);
            out.println("file number:  " + fileNumber);
            out.println("file size:    " + readable.size());
            out.println("treated as:   " + (sealed ? "sealed" : "last log"));

            long[] counts = new long[2]; // records, sync points
            LogReader.Result result = LogReader.read(readable, file.toString(), fileNumber, null, sealed,
                    new LogReader.Listener() {
                        @Override
                        public void record(long offset, byte[] payload) {
                            counts[0]++;
                            out.println("record at " + offset + ", " + payload.length + " bytes"
                                    + describeBatch(payload));
                            if (entries) {
                                printEntries(payload, out);
                            }
                        }

                        @Override
                        public void syncPoint(long offset, long syncedLength) {
                            counts[1]++;
                            out.println("sync point at " + offset + ": " + syncedLength + " bytes synced");
                        }
                    });

            out.println("kind:         " + (result.kind() == null ? "none (empty file)" : result.kind()));
            out.println("records:      " + counts[0]);
            out.println("sync points:  " + counts[1]);
            if (result.tornTail()) {
                out.println("end:          torn tail at " + result.failureOffset() + " ("
                        + result.failure() + "); valid length " + result.validLength());
            } else {
                out.println("end:          clean");
            }
        }
    }

    /** Returns a short description if the payload is a write batch, otherwise nothing. */
    private static String describeBatch(byte[] payload) {
        try {
            long[] count = new long[1];
            WriteBatch.decode(payload, new WriteBatch.Handler() {
                @Override
                public void put(long sequence, byte[] key, byte[] value) {
                    count[0]++;
                }

                @Override
                public void delete(long sequence, byte[] key) {
                    count[0]++;
                }
            });
            long start = WriteBatch.startSequence(payload);
            return ": batch, sequence " + start
                    + (count[0] > 1 ? " to " + (start + count[0] - 1) : "") + ", " + count[0]
                    + (count[0] == 1 ? " entry" : " entries");
        } catch (DecodeException e) {
            return ": not a write batch";
        }
    }

    private static void printEntries(byte[] payload, PrintStream out) {
        try {
            WriteBatch.decode(payload, new WriteBatch.Handler() {
                @Override
                public void put(long sequence, byte[] key, byte[] value) {
                    out.println("  @" + sequence + " PUT " + HEX.formatHex(key) + " -> " + describeValue(value));
                }

                @Override
                public void delete(long sequence, byte[] key) {
                    out.println("  @" + sequence + " DELETE " + HEX.formatHex(key));
                }
            });
        } catch (DecodeException e) {
            out.println("  (cannot decode: " + e.getMessage() + ")");
        }
    }

    private static String describeValue(byte[] value) {
        if (value.length <= MAX_VALUE_BYTES_SHOWN) {
            return HEX.formatHex(value) + " (" + value.length + " bytes)";
        }
        return HEX.formatHex(value, 0, MAX_VALUE_BYTES_SHOWN) + "... (" + value.length + " bytes)";
    }
}
