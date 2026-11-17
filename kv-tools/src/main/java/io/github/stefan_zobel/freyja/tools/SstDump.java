package io.github.stefan_zobel.freyja.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;

import io.github.stefan_zobel.freyja.lsm.fs.RealFileSystem;
import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;
import io.github.stefan_zobel.freyja.lsm.table.Block;
import io.github.stefan_zobel.freyja.lsm.table.BlockHandle;
import io.github.stefan_zobel.freyja.lsm.table.InternalKey;
import io.github.stefan_zobel.freyja.lsm.table.SSTableReader;
import io.github.stefan_zobel.freyja.lsm.table.SSTableReader.DataBlockInfo;
import io.github.stefan_zobel.freyja.lsm.table.SSTableReader.TableIterator;
import io.github.stefan_zobel.freyja.lsm.table.TableFormat;

/**
 * Offline tool that prints the structure and, on request, the entries of a table file.
 *
 * <p>Every block is verified while it is read. The tool stops with an error at the first
 * unit that fails verification.
 *
 * <pre>
 * usage: SstDump [--file-number N] [--entries] FILE
 * </pre>
 *
 * <p>Checksums are bound to the file number. By default it is taken from the file name
 * ({@code 000042.sst} is file 42); {@code --file-number} overrides that for a renamed file.
 */
public final class SstDump {

    private static final HexFormat HEX = HexFormat.of();
    private static final int MAX_VALUE_BYTES_SHOWN = 32;

    private SstDump() {
    }

    public static void main(String[] args) {
        System.exit(run(args, new RealFileSystem(), System.out, System.err));
    }

    /**
     * Runs the tool.
     *
     * @return the process exit code: 0 on success, 1 if the file could not be read or failed
     *         verification, 2 on a usage error
     */
    public static int run(String[] args, StorageFileSystem fs, PrintStream out, PrintStream err) {
        Path file = null;
        Long fileNumber = null;
        boolean entries = false;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("--entries")) {
                entries = true;
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
            fileNumber = fileNumberFromName(file);
            if (fileNumber == null) {
                return usage(err, "cannot derive the file number from the name "
                        + file.getFileName() + "; pass --file-number");
            }
        }
        try {
            dump(fs, file, fileNumber, entries, out);
            return 0;
        } catch (IOException e) {
            err.println("error: " + e.getMessage());
            return 1;
        }
    }

    private static int usage(PrintStream err, String problem) {
        err.println("error: " + problem);
        err.println("usage: SstDump [--file-number N] [--entries] FILE");
        return 2;
    }

    /** Returns the number in a name like {@code 000042.sst}, or {@code null}. */
    static Long fileNumberFromName(Path file) {
        String name = file.getFileName().toString();
        int dot = name.indexOf('.');
        String digits = (dot >= 0) ? name.substring(0, dot) : name;
        if (digits.isEmpty() || digits.length() > 18 || !digits.chars().allMatch(c -> c >= '0' && c <= '9')) {
            return null;
        }
        return Long.parseLong(digits);
    }

    /**
     * Prints the description of one table.
     *
     * @param entries whether to print every entry in addition to the structure
     */
    public static void dump(StorageFileSystem fs, Path file, long fileNumber, boolean entries,
                            PrintStream out) throws IOException {
        SSTableReader reader = SSTableReader.open(fs, file, fileNumber);
        try {
            out.println("file:            " + file);
            out.println("file number:     " + fileNumber);
            out.println("file size:       " + reader.fileSize());
            out.println("format version:  " + reader.formatVersion());
            out.println("filter block:    " + describe(reader.filterHandle())
                    + ", " + reader.filterBucketCount() + " buckets, "
                    + reader.filterProbeCount() + " probes");
            out.println("index block:     " + describe(reader.indexHandle()));
            out.println("footer:          offset " + (reader.fileSize() - TableFormat.FOOTER_LENGTH)
                    + ", size " + TableFormat.FOOTER_LENGTH);

            List<DataBlockInfo> blocks = reader.dataBlocks();
            out.println("data blocks:     " + blocks.size());
            long totalEntries = 0;
            for (int i = 0; i < blocks.size(); i++) {
                DataBlockInfo block = blocks.get(i);
                // Reading the block verifies its checksum.
                int count = countEntries(reader.readBlock(block.handle()));
                totalEntries += count;
                out.println("  block " + i + ": " + describe(block.handle()) + ", " + count
                        + " entries, last key " + describeKey(block.lastKey()));
            }
            out.println("entries:         " + totalEntries);

            if (entries) {
                try (TableIterator it = reader.iterator()) {
                    for (it.seekToFirst(); it.valid(); it.next()) {
                        out.println("  " + describeKey(it.key()) + " -> " + describeValue(it.value()));
                    }
                }
            }
        } finally {
            reader.release();
        }
    }

    private static int countEntries(byte[] contents) {
        Block.Iterator it = new Block(contents).iterator(InternalKey.COMPARATOR);
        int count = 0;
        for (it.seekToFirst(); it.valid(); it.next()) {
            count++;
        }
        return count;
    }

    private static String describe(BlockHandle handle) {
        return "offset " + handle.offset() + ", size " + handle.size();
    }

    /** Formats an internal key as {@code hex-user-key @sequence TYPE}. */
    static String describeKey(byte[] internalKey) {
        if (!InternalKey.isWellFormed(internalKey)) {
            return "malformed:" + HEX.formatHex(internalKey);
        }
        String type = InternalKey.type(internalKey) == InternalKey.TYPE_VALUE ? "VALUE" : "TOMBSTONE";
        return HEX.formatHex(internalKey, 0, InternalKey.userKeyLength(internalKey))
                + " @" + InternalKey.sequence(internalKey) + " " + type;
    }

    private static String describeValue(byte[] value) {
        if (value.length <= MAX_VALUE_BYTES_SHOWN) {
            return HEX.formatHex(value) + " (" + value.length + " bytes)";
        }
        return HEX.formatHex(value, 0, MAX_VALUE_BYTES_SHOWN) + "... (" + value.length + " bytes)";
    }
}
