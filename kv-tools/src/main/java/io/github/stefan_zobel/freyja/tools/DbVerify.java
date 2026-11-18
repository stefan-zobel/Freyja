package io.github.stefan_zobel.freyja.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import io.github.stefan_zobel.freyja.lsm.EngineVerifier;
import io.github.stefan_zobel.freyja.lsm.fs.RealFileSystem;
import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;

/**
 * Offline tool that checks an engine directory: {@code CURRENT}, the manifest, every block
 * of every table, and the logs. It changes nothing. The directory must not be in use.
 *
 * <pre>
 * usage: DbVerify [--levels N] DIRECTORY
 * </pre>
 */
public final class DbVerify {

    private DbVerify() {
    }

    public static void main(String[] args) {
        System.exit(run(args, new RealFileSystem(), System.out, System.err));
    }

    /**
     * Runs the tool.
     *
     * @return the process exit code: 0 if the directory is intact, 1 if damage was found or
     *         the directory could not be read, 2 on a usage error
     */
    public static int run(String[] args, StorageFileSystem fs, PrintStream out, PrintStream err) {
        Path dir = null;
        int levels = 7;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("--levels") && i + 1 < args.length) {
                try {
                    levels = Integer.parseInt(args[++i]);
                } catch (NumberFormatException e) {
                    return usage(err, "not a number: " + args[i]);
                }
                if (levels < 2 || levels > 16) {
                    return usage(err, "the level count must be between 2 and 16");
                }
            } else if (arg.startsWith("--") || dir != null) {
                return usage(err, "unexpected argument: " + arg);
            } else {
                dir = Path.of(arg);
            }
        }
        if (dir == null) {
            return usage(err, "no directory given");
        }
        EngineVerifier.Report report;
        try {
            report = EngineVerifier.verify(fs, dir, levels);
        } catch (IOException e) {
            err.println("error: " + e);
            return 1;
        }
        out.println("directory:    " + dir);
        out.println("tables:       " + report.tables() + " with " + report.entries() + " entries");
        out.println("logs:         " + report.logs() + " with " + report.logRecords() + " batches");
        for (String note : report.notes()) {
            out.println("note:         " + note);
        }
        for (String problem : report.problems()) {
            out.println("DAMAGE:       " + problem);
        }
        out.println("result:       " + (report.intact() ? "intact" : report.problems().size() + " problems found"));
        return report.intact() ? 0 : 1;
    }

    private static int usage(PrintStream err, String problem) {
        err.println("error: " + problem);
        err.println("usage: DbVerify [--levels N] DIRECTORY");
        return 2;
    }
}
