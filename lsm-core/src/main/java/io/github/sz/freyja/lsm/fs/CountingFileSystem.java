package io.github.sz.freyja.lsm.fs;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A file system that passes everything on to another one and counts the bytes appended to
 * files through it (docs/spec/storage-engine.md, section 10). A node reports the count, and
 * the write amplification of a benchmark run is computed from it (docs/spec/verification.md,
 * section 7).
 *
 * <p>The count is of bytes handed to the file system, not of blocks the device wrote. A file
 * made by a hard link costs nothing.
 */
public final class CountingFileSystem implements StorageFileSystem {

    private final StorageFileSystem delegate;
    private final AtomicLong appended = new AtomicLong();

    public CountingFileSystem(StorageFileSystem delegate) {
        this.delegate = delegate;
    }

    /** The bytes appended to files since this file system was made. */
    public long bytesWritten() {
        return appended.get();
    }

    @Override
    public void createDirectories(Path dir) throws IOException {
        delegate.createDirectories(dir);
    }

    @Override
    public WritableFile create(Path path) throws IOException {
        WritableFile file = delegate.create(path);
        return new WritableFile() {
            @Override
            public void append(ByteBuffer src) throws IOException {
                int length = src.remaining();
                file.append(src);
                appended.addAndGet(length);
            }

            @Override
            public void sync() throws IOException {
                file.sync();
            }

            @Override
            public long size() {
                return file.size();
            }

            @Override
            public void close() throws IOException {
                file.close();
            }
        };
    }

    @Override
    public ReadableFile open(Path path) throws IOException {
        return delegate.open(path);
    }

    @Override
    public void rename(Path source, Path target) throws IOException {
        delegate.rename(source, target);
    }

    @Override
    public void link(Path existing, Path link) throws IOException {
        delegate.link(existing, link);
    }

    @Override
    public void delete(Path path) throws IOException {
        delegate.delete(path);
    }

    @Override
    public void deleteDirectory(Path dir) throws IOException {
        delegate.deleteDirectory(dir);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return delegate.exists(path);
    }

    @Override
    public long size(Path path) throws IOException {
        return delegate.size(path);
    }

    @Override
    public void truncate(Path path, long size) throws IOException {
        delegate.truncate(path, size);
    }

    @Override
    public List<Path> list(Path dir) throws IOException {
        return delegate.list(dir);
    }

    @Override
    public void syncDirectory(Path dir) throws IOException {
        delegate.syncDirectory(dir);
    }

    @Override
    public long freeSpace(Path dir) throws IOException {
        return delegate.freeSpace(dir);
    }

    @Override
    public Closeable lock(Path path) throws IOException {
        return delegate.lock(path);
    }
}
