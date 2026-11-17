package io.github.stefan_zobel.freyja.lsm.manifest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import io.github.stefan_zobel.freyja.common.DecodeException;

/**
 * An immutable set of live table files per level, together with the counters the manifest
 * records. Applying a {@link VersionEdit} yields a new version.
 *
 * <p>Level 0 is ordered newest file first (descending file number); its files may overlap.
 */
public final class Version {

    private static final Comparator<FileMeta> NEWEST_FIRST =
            Comparator.comparingLong(FileMeta::number).reversed();

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
     * Returns the version that results from applying {@code edit}.
     *
     * @throws DecodeException if the edit contradicts this version: an unknown level, a file
     *                         added twice, a removed file that is not there, or a counter
     *                         that moves backward
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

    private void requireLevel(int level) {
        if (level < 0 || level >= levels.size()) {
            throw new DecodeException("level " + level + " does not exist; the engine has "
                    + levels.size() + " levels");
        }
    }
}
