package io.github.sz.freyja.lsm.fs;

import java.nio.file.Path;

import org.junit.jupiter.api.io.TempDir;

/** Runs the file-system contract against the real file system. */
class RealFileSystemTest extends FileSystemContract {

    @TempDir
    Path tempDir;

    private final RealFileSystem fs = new RealFileSystem();

    @Override
    StorageFileSystem fs() {
        return fs;
    }

    @Override
    Path root() {
        return tempDir;
    }
}
