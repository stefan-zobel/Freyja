package io.github.stefan_zobel.freyja.lsm.manifest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import io.github.stefan_zobel.freyja.common.ByteComparator;
import io.github.stefan_zobel.freyja.common.DecodeException;
import io.github.stefan_zobel.freyja.lsm.table.InternalKey;

/**
 * An immutable set of live table files per level, together with the counters the manifest
 * records. Applying a {@link VersionEdit} yields a new version.
 *
 * <ul>
 *   <li>Level 0 is ordered newest file first (descending file number); its files may overlap.</li>
 *   <li>Every deeper level is ordered by key. Its files do not overlap: no user key occurs in
 *       two files of one level.</li>
 * </ul>
 */
public final class Version {

    private static final Comparator<FileMeta> NEWEST_FIRST =
            Comparator.comparingLong(FileMeta::number).reversed();
    private static final Comparator<FileMeta> BY_SMALLEST_KEY =
            (a, b) -> InternalKey.compare(a.smallestKey(), b.smallestKey());

    private final List<List<FileMeta>> levels;
    private final long nextFileNumber;
    private final long lastSequence;
    private final long minWalNumber;

    private Version(List<List<FileMeta>> levels, long nextFileNumber, long lastSequence,
                    long minWalNumber) {
        this.levels = levels;
        this.nextFileNumber = nextFileNumber;
        this.lastSequence = lastSequence;
        this.minWalNumber = minWalNumber;
    }

    /** Returns the version of a new, empty engine. */
    public static Version empty(int levelCount) {
        List<List<FileMeta>> levels = new ArrayList<>();
        for (int i = 0; i < levelCount; i++) {
            levels.add(List.of());
        }
        return new Version(Collections.unmodifiableList(levels), 1, 0, 0);
    }

    /**
     * Returns the version that results from applying {@code edit}. Removals are applied
     * before additions.
     *
     * @throws DecodeException if the edit contradicts this version: an unknown level, a file
     *                         added twice, a removed file that is not there, overlapping
     *                         files in a level below 0, or a counter that moves backward
     */
    public Version apply(VersionEdit edit) {
        List<List<FileMeta>> next = new ArrayList<>();
        for (List<FileMeta> level : levels) {
            next.add(new ArrayList<>(level));
        }
        for (VersionEdit.RemovedFile removed : edit.removedFiles()) {
            requireLevel(removed.level());
            if (!next.get(removed.level()).removeIf(f -> f.number() == removed.number())) {
                throw new DecodeException("edit removes file " + removed.number()
                        + ", which is not in level " + removed.level());
            }
        }
        for (VersionEdit.AddedFile added : edit.addedFiles()) {
            requireLevel(added.level());
            for (List<FileMeta> level : next) {
                for (FileMeta existing : level) {
                    if (existing.number() == added.file().number()) {
                        throw new DecodeException("edit adds file " + existing.number() + " twice");
                    }
                }
            }
            next.get(added.level()).add(added.file());
        }
        next.get(0).sort(NEWEST_FIRST);
        for (int level = 1; level < next.size(); level++) {
            List<FileMeta> files = next.get(level);
            files.sort(BY_SMALLEST_KEY);
            for (int i = 1; i < files.size(); i++) {
                if (compareUserKeys(files.get(i - 1).largestKey(), files.get(i).smallestKey()) >= 0) {
                    throw new DecodeException("files " + files.get(i - 1).number() + " and "
                            + files.get(i).number() + " overlap in level " + level);
                }
            }
        }
        for (int i = 0; i < next.size(); i++) {
            next.set(i, Collections.unmodifiableList(next.get(i)));
        }

        long newNext = edit.nextFileNumber() != null ? edit.nextFileNumber() : nextFileNumber;
        long newSequence = edit.lastSequence() != null ? edit.lastSequence() : lastSequence;
        long newMinWal = edit.minWalNumber() != null ? edit.minWalNumber() : minWalNumber;
        if (newNext < nextFileNumber || newSequence < lastSequence || newMinWal < minWalNumber) {
            throw new DecodeException("a manifest counter moves backward");
        }
        return new Version(Collections.unmodifiableList(next), newNext, newSequence, newMinWal);
    }

    /** Returns one edit that describes this version completely. */
    public VersionEdit toEdit() {
        VersionEdit edit = new VersionEdit()
                .nextFileNumber(nextFileNumber)
                .lastSequence(lastSequence)
                .minWalNumber(minWalNumber);
        for (int level = 0; level < levels.size(); level++) {
            for (FileMeta file : levels.get(level)) {
                edit.addFile(level, file);
            }
        }
        return edit;
    }

    /** Returns the files of {@code level}. */
    public List<FileMeta> files(int level) {
        return levels.get(level);
    }

    /** Returns every file of every level. */
    public List<FileMeta> allFiles() {
        List<FileMeta> all = new ArrayList<>();
        for (List<FileMeta> level : levels) {
            all.addAll(level);
        }
        return all;
    }

    /**
     * Returns the files of {@code level} whose user-key range intersects the closed range
     * {@code [smallestUserKey, largestUserKey]}, in the order of the level.
     */
    public List<FileMeta> overlapping(int level, byte[] smallestUserKey, byte[] largestUserKey) {
        List<FileMeta> result = new ArrayList<>();
        for (FileMeta file : levels.get(level)) {
            byte[] fileSmallest = file.smallestKey();
            byte[] fileLargest = file.largestKey();
            boolean before = ByteComparator.compare(fileLargest, 0, InternalKey.userKeyLength(fileLargest),
                    smallestUserKey, 0, smallestUserKey.length) < 0;
            boolean after = ByteComparator.compare(fileSmallest, 0, InternalKey.userKeyLength(fileSmallest),
                    largestUserKey, 0, largestUserKey.length) > 0;
            if (!before && !after) {
                result.add(file);
            }
        }
        return result;
    }

    /**
     * Returns the file of a level below 0 whose user-key range includes {@code userKey}, or
     * {@code null}. At most one file can.
     */
    public FileMeta fileContaining(int level, byte[] userKey) {
        List<FileMeta> files = levels.get(level);
        int low = 0;
        int high = files.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            FileMeta file = files.get(mid);
            byte[] largest = file.largestKey();
            if (ByteComparator.compare(largest, 0, InternalKey.userKeyLength(largest),
                    userKey, 0, userKey.length) < 0) {
                low = mid + 1;
            } else if (file.mayContainUserKey(userKey)) {
                return file;
            } else {
                high = mid - 1;
            }
        }
        return null;
    }

    /** Returns the total size of the files of {@code level} in bytes. */
    public long levelBytes(int level) {
        long total = 0;
        for (FileMeta file : levels.get(level)) {
            total += file.size();
        }
        return total;
    }

    public int levelCount() {
        return levels.size();
    }

    /** Returns the smallest file number that has never been used. */
    public long nextFileNumber() {
        return nextFileNumber;
    }

    /** Returns the last sequence number contained in the table files. */
    public long lastSequence() {
        return lastSequence;
    }

    /** Returns the WAL number below which all WALs are obsolete. */
    public long minWalNumber() {
        return minWalNumber;
    }

    /** Compares the user keys of two internal keys. */
    private static int compareUserKeys(byte[] a, byte[] b) {
        return ByteComparator.compare(a, 0, InternalKey.userKeyLength(a), b, 0, InternalKey.userKeyLength(b));
    }

    private void requireLevel(int level) {
        if (level < 0 || level >= levels.size()) {
            throw new DecodeException("level " + level + " does not exist; the engine has "
                    + levels.size() + " levels");
        }
    }
}
