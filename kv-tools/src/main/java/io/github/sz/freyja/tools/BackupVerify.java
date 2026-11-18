package io.github.sz.freyja.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.TreeSet;

import io.github.sz.freyja.lsm.fs.RealFileSystem;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.node.backup.Backups;

/**
 * Checks a backup completely before it is relied on (docs/spec/operations.md, section 3):
 * the manifest, every file against its length and its checksum, and the storage engine in it
 * by its own checksums. It changes nothing.
 *
 * <pre>
 * usage: BackupVerify BACKUP-DIRECTORY
 * </pre>
 *
 * <p>Exit code 0: the backup is whole. 1: it is not, or an I/O error. 2: wrong usage.
 */
public final class BackupVerify {

    private BackupVerify() {
    }

    public static void main(String[] args) {
        System.exit(run(args, new RealFileSystem(), System.out, System.err));
    }

    public static int run(String[] args, StorageFileSystem fs, PrintStream out, PrintStream err) {
        if (args.length != 1 || args[0].startsWith("--")) {
            err.println("usage: BackupVerify BACKUP-DIRECTORY");
            return 2;
        }
        Path backup = Path.of(args[0]);
        try {
            Backups.Manifest manifest = Backups.readManifest(fs, backup);
            out.println("backup of:    node " + manifest.nodeId() + " of cluster " + Long.toHexString(manifest.clusterId())
                    + ", cluster version " + manifest.clusterVersion());
            out.println("covers:       the log through index " + manifest.meta().lastIncludedIndex() + " of term "
                    + manifest.meta().lastIncludedTerm() + ", voters "
                    + new TreeSet<>(manifest.meta().config().voters()) + ", learners "
                    + new TreeSet<>(manifest.meta().config().learners()));
            long bytes = manifest.files().stream().mapToLong(Backups.BackupFile::length).sum();
            out.println("files:        " + manifest.files().size() + ", " + bytes + " bytes");
            List<String> wrong = Backups.verify(fs, backup);
            for (String line : wrong) {
                out.println("DAMAGE:       " + line);
            }
            boolean whole = wrong.isEmpty();
            if (whole) {
                // The files are what was written. Is what was written a sound engine?
                ByteArrayOutputStream detail = new ByteArrayOutputStream();
                PrintStream quiet = new PrintStream(detail, true, StandardCharsets.UTF_8);
                if (DbVerify.run(new String[] {backup.resolve(Backups.ENGINE).toString()}, fs, quiet, quiet) != 0) {
                    whole = false;
                    out.println("DAMAGE:       the storage engine in the backup does not verify");
                    detail.toString(StandardCharsets.UTF_8).lines().limit(10)
                            .forEach(line -> out.println("              " + line));
                }
            }
            out.println("verdict:      " + (whole ? "the backup is whole" : "the backup must not be relied on"));
            return whole ? 0 : 1;
        } catch (IOException e) {
            err.println("error: " + e.getMessage());
            return 1;
        }
    }
}
