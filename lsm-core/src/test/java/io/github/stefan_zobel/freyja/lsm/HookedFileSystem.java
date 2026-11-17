package io.github.stefan_zobel.freyja.lsm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.stefan_zobel.freyja.lsm.fs.ReadableFile;
import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;
import io.github.stefan_zobel.freyja.lsm.fs.WritableFile;

/**
 * A delegating file system for tests that counts syncs and can slow them down or make
 * syncs and appends fail. The complete fault-injecting file system is a later milestone.
 */
final class HookedFileSystem implements StorageFileSystem {

    private final StorageFileSystem delegate;

    final AtomicInteger syncCount = new AtomicInteger();
    volatile long syncDelayMillis;
    volatile boolean failSyncs;
    volatile boolean failAppends;

    HookedFileSystem(StorageFileSystem delegate) {
        this.delegate = delegate;
    }

    @Override
    public WritableFile create(Path path) throws IOException {
        WritableFile file = delegate.create(path);
        return new WritableFile() {
            @Override
            public void append(ByteBuffer src) throws IOException {
                if (failAppends) {
                    throw new IOException("injected append failure");
                }
                file.append(src);
            }

            @Override
            public void sync() throws IOException {
                if (failSyncs) {
                    throw new IOException("injected sync failure");
                }
                long delay = syncDelayMillis;
                if (delay > 0) {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                syncCount.incrementAndGet();
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
    public void createDirectories(Path dir) throws IOException {
        delegate.createDirectories(dir);
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
    public java.io.Closeable lock(Path path) throws IOException {
        return delegate.lock(path);
    }
}
