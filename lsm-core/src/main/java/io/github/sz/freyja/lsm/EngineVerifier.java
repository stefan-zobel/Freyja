package io.github.sz.freyja.lsm;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.lsm.fs.ReadableFile;
import io.github.sz.freyja.lsm.fs.StorageFileSystem;
import io.github.sz.freyja.lsm.log.FileKind;
import io.github.sz.freyja.lsm.log.LogReader;
import io.github.sz.freyja.lsm.manifest.CurrentFile;
import io.github.sz.freyja.lsm.manifest.FileMeta;
import io.github.sz.freyja.lsm.manifest.FileNames;
import io.github.sz.freyja.lsm.manifest.Version;
import io.github.sz.freyja.lsm.manifest.VersionEdit;
import io.github.sz.freyja.lsm.table.SSTableReader;

/**
 * Checks an engine directory offline, without changing anything in it: {@code CURRENT}, the
 * manifest, every block of every table the manifest lists, and the logs.
 *
 * <p>The directory must not be in use by a running engine. Unlike opening the engine, the
 * check takes no lock, replays nothing into memory, and deletes nothing, so it is safe on a
 * directory that is suspected to be damaged.
 */
public final class EngineVerifier {

    /**
     * The outcome of a check.
     *
     * @param tables     the number of table files the manifest lists
     * @param entries    the number of entries in the tables that could be read
     * @param logs       the number of logs that are not obsolete
     * @param logRecords the number of batches in those logs
     * @param problems   what is damaged or missing; empty if the directory is intact
     * @param notes      observations that are no damage, such as a torn log tail
     */
    public record Report(int tables, long entries, int logs, long logRecords,
                         List<String> problems, List<String> notes) {

        /** Returns whether no damage was found. */
        public boolean intact() {
            return problems.isEmpty();
        }
    }

    private EngineVerifier() {
    }

    /**
     * Checks the engine directory {@code dir}.
     *
     * @param levelCount the number of levels the engine is configured with
     * @throws IOException if the directory cannot be read at all
     */
    public static Report verify(StorageFileSystem fs, Path dir, int levelCount) throws IOException {
        List<String> problems = new ArrayList<>();
        List<String> notes = new ArrayList<>();
        TreeMap<Long, Path> logs = new TreeMap<>();
        TreeMap<Long, Path> tableFiles = new TreeMap<>();
        for (Path path : fs.list(dir)) {
            FileNames.Numbered file = FileNames.parse(path);
            if (file != null && file.kind() == FileNames.Kind.WAL) {
                logs.put(file.number(), path);
            } else if (file != null && file.kind() == FileNames.Kind.TABLE) {
                tableFiles.put(file.number(), path);
            }
        }

        if (!fs.exists(dir.resolve(FileNames.CURRENT))) {
            if (logs.isEmpty() && tableFiles.isEmpty()) {
                notes.add("no CURRENT file and no data: an empty or new directory");
            } else {
                problems.add("CURRENT is missing, but the directory holds log or table files");
            }
            return new Report(0, 0, 0, 0, problems, notes);
        }

        // CURRENT and the manifest.
        Version[] version = {Version.empty(levelCount)};
        int[] edits = {0};
        long manifestNumber;
        try {
            manifestNumber = CurrentFile.read(fs, dir);
            Path manifestPath = FileNames.manifest(dir, manifestNumber);
            String manifestName = manifestPath.toString();
            try (ReadableFile file = fs.open(manifestPath)) {
                LogReader.Result result = LogReader.read(file, manifestName, manifestNumber,
                        FileKind.MANIFEST, false, (offset, payload) -> {
                            try {
                                version[0] = version[0].apply(VersionEdit.decode(payload));
                                edits[0]++;
                            } catch (VersionEdit.UnknownTagException e) {
                                throw new UnsupportedFormatException("manifest " + manifestName + ": " + e.getMessage());
                            } catch (DecodeException e) {
                                throw new CorruptionException(manifestName, offset, "malformed edit: " + e.getMessage(), e);
                            }
                        });
                if (result.tornTail()) {
                    notes.add("the manifest ends in a torn tail at " + result.failureOffset()
                            + "; edits after it never took effect");
                }
            } catch (NoSuchFileException e) {
                throw new CorruptionException(manifestName, 0, "the manifest named by CURRENT is missing");
            }
            if (edits[0] == 0) {
                throw new CorruptionException(manifestName, 0, "the manifest named by CURRENT holds no edit");
            }
        } catch (CorruptionException | UnsupportedFormatException e) {
            problems.add(e.getMessage());
            return new Report(0, 0, 0, 0, problems, notes);
        }

        // The tables.
        long entries = 0;
        List<FileMeta> files = version[0].allFiles();
        for (FileMeta file : files) {
            Path path = FileNames.table(dir, file.number());
            tableFiles.remove(file.number());
            try {
                long size = fs.size(path);
                if (size != file.size()) {
                    problems.add("corruption in " + path + ": the table has " + size
                            + " bytes, the manifest records " + file.size());
                    continue;
                }
                SSTableReader reader = SSTableReader.open(fs, path, file.number());
                try {
                    entries += reader.verify(null);
                } finally {
                    reader.release();
                }
            } catch (NoSuchFileException e) {
                problems.add("corruption in " + path + ": a table listed in the manifest is missing");
            } catch (CorruptionException | UnsupportedFormatException e) {
                problems.add(e.getMessage());
            }
        }
        for (Path orphan : tableFiles.values()) {
            notes.add(orphan.getFileName() + " is not listed in the manifest; the engine deletes it at open");
        }

        // The logs that the tables do not cover yet.
        long[] sequence = {version[0].lastSequence()};
        long[] records = {0};
        var live = logs.tailMap(version[0].minWalNumber(), true);
        for (Path obsolete : logs.headMap(version[0].minWalNumber(), false).values()) {
            notes.add(obsolete.getFileName() + " is obsolete; the engine deletes it at open");
        }
        for (var entry : live.entrySet()) {
            boolean sealed = !entry.getKey().equals(live.lastKey());
            String name = entry.getValue().toString();
            try (ReadableFile file = fs.open(entry.getValue())) {
                LogReader.Result result = LogReader.read(file, name, entry.getKey(), FileKind.WAL, sealed,
                        (offset, payload) -> {
                            try {
                                long start = WriteBatch.startSequence(payload);
                                long count = WriteBatch.entryCount(payload);
                                if (start != sequence[0] + 1 || count == 0) {
                                    throw new DecodeException("batch starts at sequence " + start + " with "
                                            + count + " entries, expected " + (sequence[0] + 1));
                                }
                                WriteBatch.decode(payload, new WriteBatch.Handler() {
                                    @Override
                                    public void put(long seq, byte[] key, byte[] value) {
                                    }

                                    @Override
                                    public void delete(long seq, byte[] key) {
                                    }
                                });
                                sequence[0] = start + count - 1;
                                records[0]++;
                            } catch (DecodeException e) {
                                throw new CorruptionException(name, offset, "malformed batch: " + e.getMessage(), e);
                            }
                        });
                if (result.tornTail()) {
                    notes.add(entry.getValue().getFileName() + " ends in a torn tail at "
                            + result.failureOffset() + "; the engine cuts it at open");
                }
            } catch (CorruptionException | UnsupportedFormatException e) {
                problems.add(e.getMessage());
                break; // later logs cannot continue a damaged history
            }
        }
        return new Report(files.size(), entries, live.size(), records[0], problems, notes);
    }
}
