package io.github.stefan_zobel.freyja.lsm.log;

/**
 * What a file in the WAL record format contains. The kind is stored in the header record, so
 * a file that ends up under the wrong name is recognized.
 */
public enum FileKind {

    /** Write-ahead log of the storage engine. */
    WAL(1),

    /** Manifest of the storage engine. */
    MANIFEST(2),

    /** Segment of the Raft log. */
    RAFT_LOG(3);

    private final int code;

    FileKind(int code) {
        this.code = code;
    }

    /** Returns the byte stored in the header record. */
    public int code() {
        return code;
    }

    /** Returns the kind for a stored byte, or {@code null} if the byte is unknown. */
    public static FileKind fromCode(int code) {
        for (FileKind kind : values()) {
            if (kind.code == code) {
                return kind;
            }
        }
        return null;
    }
}
