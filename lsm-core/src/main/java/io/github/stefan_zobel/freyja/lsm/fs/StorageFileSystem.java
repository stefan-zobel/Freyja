package io.github.stefan_zobel.freyja.lsm.fs;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * The only way storage code reaches files (docs/spec/storage-engine.md, section 1).
 *
 * <p>Durability model, which every implementation follows and every caller must assume:
 * <ul>
 *   <li>Appended bytes are durable only after {@link WritableFile#sync()} returned.</li>
 *   <li>A created, renamed, linked, or deleted name is durable only after
 *       {@link #syncDirectory(Path)} of its directory returned.</li>
 * </ul>
 *
 * <p>Callers must close every handle to a file before deleting it, renaming it, or renaming
 * another file over it. Implementations may reject such calls.
 *
 * <p>All methods are safe for use by several threads.
 */
public interface StorageFileSystem {

    /** Creates {@code dir} and every missing parent. Does nothing if it already exists. */
    void createDirectories(Path dir) throws IOException;

    /**
     * Creates a new, empty file for appending.
     *
     * @throws java.nio.file.FileAlreadyExistsException if {@code path} exists
     */
    WritableFile create(Path path) throws IOException;

    /**
     * Opens an existing file for positional reads.
     *
     * @throws java.nio.file.NoSuchFileException if {@code path} does not exist
     */
    ReadableFile open(Path path) throws IOException;

    /** Atomically renames {@code source} to {@code target}, replacing {@code target} if it exists. */
    void rename(Path source, Path target) throws IOException;

    /**
     * Creates {@code link} as a second name for the file {@code existing}. Both must be on
     * the same file system.
     *
     * @throws java.nio.file.FileAlreadyExistsException if {@code link} exists
     */
    void link(Path existing, Path link) throws IOException;

    /**
     * Deletes the name {@code path}.
     *
     * @throws java.nio.file.NoSuchFileException if {@code path} does not exist
     */
    void delete(Path path) throws IOException;

    /**
     * Removes the empty directory {@code dir}. The removal of the names it held is durable
     * with it.
     *
     * @throws java.nio.file.NoSuchFileException if {@code dir} does not exist
     * @throws java.nio.file.DirectoryNotEmptyException if it still has entries
     */
    void deleteDirectory(Path dir) throws IOException;

    /** Returns whether a file or directory with this name exists. */
    boolean exists(Path path) throws IOException;

    /** Returns the size of the file in bytes. */
    long size(Path path) throws IOException;

    /**
     * Cuts the file to {@code size} bytes and makes the new length durable. The file must not
     * be open, and {@code size} must not exceed the current size.
     */
    void truncate(Path path, long size) throws IOException;

    /** Returns the entries of {@code dir} as full paths, sorted by name. */
    List<Path> list(Path dir) throws IOException;

    /** Makes the names in {@code dir} durable. A no-op on platforms without directory sync. */
    void syncDirectory(Path dir) throws IOException;

    /** Returns the number of bytes available for new data in the file system of {@code dir}. */
    long freeSpace(Path dir) throws IOException;

    /**
     * Takes an exclusive lock on the file {@code path}, creating the file if needed. The lock
     * keeps a second owner, in this or another process, from using the same directory.
     *
     * @return a handle that releases the lock when closed; the lock file stays in place
     * @throws IOException if the lock is held by someone else
     */
    Closeable lock(Path path) throws IOException;
}
