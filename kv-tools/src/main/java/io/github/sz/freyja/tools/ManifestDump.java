package io.github.sz.freyja.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.lsm.CorruptionException;
import io.github.sz.freyja.lsm.UnsupportedFormatException;
import io.github.sz.freyja.lsm.fs.ReadableFile;
import io.github.sz.freyja.lsm.fs.RealFileSystem;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.lsm.log.FileKind;
import io.github.sz.freyja.lsm.log.LogReader;
import io.github.sz.freyja.lsm.manifest.FileMeta;
import io.github.sz.freyja.lsm.manifest.FileNames;
import io.github.sz.freyja.lsm.manifest.Version;
import io.github.sz.freyja.lsm.manifest.VersionEdit;

/**
 * Offline tool that prints the edits of a manifest and the version they add up to.
 *
 * <pre>
 * usage: ManifestDump [--file-number N] [--levels N] FILE
 * </pre>
 *
 * <ul>
 *   <li>{@code --file-number}: checksums are bound to the file number. By default it is
 *       taken from the file name ({@code MANIFEST-000042} is file 42).</li>
 *   <li>{@code --levels}: the number of levels of the engine; default 7.</li>
 * </ul>
 *
 * <p>The tool never changes the file.
 */
public final class ManifestDump {

    private ManifestDump() {
    }

    public static void main(String[] args) {
        System.exit(run(args, new RealFileSystem(), System.out, System.err));
    }

    /**
     * Runs the tool.
     *
     * @return the process exit code: 0 if the manifest is readable (a torn tail included),
     *         1 if it could not be read or is damaged, 2 on a usage error
     */
    public static int run(String[] args, StorageFileSystem fs, PrintStream out, PrintStream err) {
        Path file = null;
        Long fileNumber = null;
        int levels = 7;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ((arg.equals("--file-number") || arg.equals("--levels")) && i + 1 < args.length) {
                try {
                    long value = Long.parseLong(args[++i]);
                    if (arg.equals("--levels")) {
                        if (value < 2 || value > 16) {
                            return usage(err, "the level count must be between 2 and 16");
                        }
                        levels = (int) value;
                    } else {
                        fileNumber = value;
                    }
                } catch (NumberFormatException e) {
                    return usage(err, "not a number: " + args[i]);
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
            FileNames.Numbered parsed = FileNames.parse(file);
            if (parsed == null || parsed.kind() != FileNames.Kind.MANIFEST) {
                return usage(err, "cannot derive the file number from the name "
                        + file.getFileName() + "; pass --file-number");
            }
            fileNumber = parsed.number();
        }
        try {
            dump(fs, file, fileNumber, levels, out);
            return 0;
        } catch (IOException e) {
            err.println("error: " + e.getMessage());
            return 1;
        }
    }

    private static int usage(PrintStream err, String problem) {
        err.println("error: " + problem);
        err.println("usage: ManifestDump [--file-number N] [--levels N] FILE");
        return 2;
    }

    /** Prints every edit of one manifest and the resulting version. */
    public static void dump(StorageFileSystem fs, Path file, long fileNumber, int levels,
                            PrintStream out) throws IOException {
        String name = file.toString();
        Version[] version = {Version.empty(levels)};
        int[] edits = {0};
        try (ReadableFile readable = fs.open(file)) {
            out.println("file:         " + file);
            out.println("file number:  " + fileNumber);
            out.println("file size:    " + readable.size());

            LogReader.Result result = LogReader.read(readable, name, fileNumber, FileKind.MANIFEST, false,
                    (offset, payload) -> {
                        VersionEdit edit;
                        try {
                            edit = VersionEdit.decode(payload);
                            version[0] = version[0].apply(edit);
                        } catch (VersionEdit.UnknownTagException e) {
                            throw new UnsupportedFormatException("manifest " + name + ": " + e.getMessage());
                        } catch (DecodeException e) {
                            throw new CorruptionException(name, offset, "malformed edit: " + e.getMessage(), e);
                        }
                        edits[0]++;
                        out.println("edit " + edits[0] + " at " + offset + ":");
                        printEdit(edit, out);
                    });

            out.println("edits:        " + edits[0]);
            if (result.tornTail()) {
                out.println("end:          torn tail at " + result.failureOffset() + " ("
                        + result.failure() + "); valid length " + result.validLength());
            } else {
                out.println("end:          clean");
            }
        }

        Version result = version[0];
        out.println("resulting version:");
        out.println("  next file number:  " + result.nextFileNumber());
        out.println("  last sequence:     " + result.lastSequence());
        out.println("  min WAL number:    " + result.minWalNumber());
        for (int level = 0; level < result.levelCount(); level++) {
            if (result.files(level).isEmpty()) {
                continue;
            }
            out.println("  level " + level + ": " + result.files(level).size() + " files");
            for (FileMeta meta : result.files(level)) {
                out.println("    " + describe(meta));
            }
        }
    }

    private static void printEdit(VersionEdit edit, PrintStream out) {
        if (edit.nextFileNumber() != null) {
            out.println("  next file number: " + edit.nextFileNumber());
        }
        if (edit.lastSequence() != null) {
            out.println("  last sequence:    " + edit.lastSequence());
        }
        if (edit.minWalNumber() != null) {
            out.println("  min WAL number:   " + edit.minWalNumber());
        }
        for (VersionEdit.RemovedFile removed : edit.removedFiles()) {
            out.println("  remove level " + removed.level() + ": file " + removed.number());
        }
        for (VersionEdit.AddedFile added : edit.addedFiles()) {
            out.println("  add level " + added.level() + ": " + describe(added.file()));
        }
    }

    private static String describe(FileMeta meta) {
        return "file " + meta.number() + ", " + meta.size() + " bytes, keys ["
                + SstDump.describeKey(meta.smallestKey()) + " .. " + SstDump.describeKey(meta.largestKey())
                + "], sequences " + meta.smallestSequence() + " to " + meta.largestSequence();
    }
}
