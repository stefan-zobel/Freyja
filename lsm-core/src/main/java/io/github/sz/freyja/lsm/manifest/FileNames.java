package io.github.sz.freyja.lsm.manifest;

import java.nio.file.Path;

/**
 * Names of the files in an engine directory (docs/spec/storage-format.md): {@code NNNNNN.wal},
 * {@code NNNNNN.sst}, {@code MANIFEST-NNNNNN}, {@code CURRENT}, {@code LOCK}.
 */
public final class FileNames {

    /** Name of the file that points to the active manifest. */
    public static final String CURRENT = "CURRENT";

    /** Name of the temporary file from which {@code CURRENT} is replaced. */
    public static final String CURRENT_TMP = "CURRENT.tmp";

    /** Name of the lock file. */
    public static final String LOCK = "LOCK";

    /** Name of the file that keeps space in reserve for the case that the disk fills up. */
    public static final String RESERVE = "RESERVE";

    private static final String WAL_SUFFIX = ".wal";
    private static final String TABLE_SUFFIX = ".sst";
    private static final String MANIFEST_PREFIX = "MANIFEST-";

    /** The kinds of numbered files. */
    public enum Kind { WAL, TABLE, MANIFEST }

    /**
     * A numbered file.
     *
     * @param kind   what the file is
     * @param number its file number
     */
    public record Numbered(Kind kind, long number) {
    }

    private FileNames() {
    }

    public static Path wal(Path dir, long number) {
        return dir.resolve(String.format("%06d%s", number, WAL_SUFFIX));
    }

    public static Path table(Path dir, long number) {
        return dir.resolve(String.format("%06d%s", number, TABLE_SUFFIX));
    }

    public static Path manifest(Path dir, long number) {
        return dir.resolve(manifestName(number));
    }

    public static String manifestName(long number) {
        return String.format("%s%06d", MANIFEST_PREFIX, number);
    }

    /** Returns kind and number of a numbered file, or {@code null} for any other name. */
    public static Numbered parse(Path path) {
        return parse(path.getFileName().toString());
    }

    /** Returns kind and number of a numbered file name, or {@code null} for any other name. */
    public static Numbered parse(String name) {
        if (name.endsWith(WAL_SUFFIX)) {
            return numbered(Kind.WAL, name.substring(0, name.length() - WAL_SUFFIX.length()));
        }
        if (name.endsWith(TABLE_SUFFIX)) {
            return numbered(Kind.TABLE, name.substring(0, name.length() - TABLE_SUFFIX.length()));
        }
        if (name.startsWith(MANIFEST_PREFIX)) {
            return numbered(Kind.MANIFEST, name.substring(MANIFEST_PREFIX.length()));
        }
        return null;
    }

    private static Numbered numbered(Kind kind, String digits) {
        if (digits.isEmpty() || digits.length() > 18) {
            return null;
        }
        for (int i = 0; i < digits.length(); i++) {
            char c = digits.charAt(i);
            if (c < '0' || c > '9') {
                return null;
            }
        }
        return new Numbered(kind, Long.parseLong(digits));
    }
}
