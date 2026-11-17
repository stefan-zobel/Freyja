package io.github.stefan_zobel.freyja.lsm.fs;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

/** The {@link StorageFileSystem} of the operating system, based on {@link FileChannel}. */
public final class RealFileSystem implements StorageFileSystem {

    private static final boolean WINDOWS =
            System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows");

    /** Delete attempts on Windows, where scanners and indexers hold handles briefly. */
    private static final int DELETE_ATTEMPTS = 5;
    private static final long DELETE_PAUSE_MILLIS = 20;

    @Override
    public void createDirectories(Path dir) throws IOException {
        Files.createDirectories(dir);
    }

    @Override
    public WritableFile create(Path path) throws IOException {
        FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        return new RealWritableFile(channel);
    }

    @Override
    public ReadableFile open(Path path) throws IOException {
        return new RealReadableFile(FileChannel.open(path, StandardOpenOption.READ));
    }

    @Override
    public void rename(Path source, Path target) throws IOException {
        Files.move(source, target,
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void link(Path existing, Path link) throws IOException {
        Files.createLink(link, existing);
    }

    @Override
    public void delete(Path path) throws IOException {
        int attempts = WINDOWS ? DELETE_ATTEMPTS : 1;
        for (int attempt = 1; ; attempt++) {
            try {
                Files.delete(path);
                return;
            } catch (NoSuchFileException e) {
                throw e;
            } catch (FileSystemException e) {
                if (attempt >= attempts) {
                    throw e;
                }
                pause();
            }
        }
    }

    private static void pause() throws IOException {
        try {
            Thread.sleep(DELETE_PAUSE_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while retrying a delete", e);
        }
    }

    @Override
    public boolean exists(Path path) {
        return Files.exists(path);
    }

    @Override
    public long size(Path path) throws IOException {
        return Files.size(path);
    }

    @Override
    public void truncate(Path path, long size) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            if (size < 0 || size > channel.size()) {
                throw new IllegalArgumentException(
                        "cannot truncate " + path + " of size " + channel.size() + " to " + size);
            }
            channel.truncate(size);
            channel.force(true);
        }
    }

    @Override
    public List<Path> list(Path dir) throws IOException {
        List<Path> entries = new ArrayList<>();
        try (Stream<Path> stream = Files.list(dir)) {
            stream.forEach(entries::add);
        }
        Collections.sort(entries);
        return entries;
    }

    @Override
    public void syncDirectory(Path dir) throws IOException {
        if (WINDOWS) {
            return; // Windows offers no directory sync.
        }
        try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    @Override
    public long freeSpace(Path dir) throws IOException {
        return Files.getFileStore(dir).getUsableSpace();
    }

    @Override
    public Closeable lock(Path path) throws IOException {
        FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock lock;
        try {
            lock = channel.tryLock();
        } catch (OverlappingFileLockException e) {
            lock = null; // held by this process
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }
        if (lock == null) {
            channel.close();
            throw new IOException("the lock " + path + " is held by another owner");
        }
        return channel; // closing the channel releases the lock
    }

    private static final class RealWritableFile implements WritableFile {
        private final FileChannel channel;
        private long size;

        RealWritableFile(FileChannel channel) {
            this.channel = channel;
        }

        @Override
        public void append(ByteBuffer src) throws IOException {
            while (src.hasRemaining()) {
                size += channel.write(src);
            }
        }

        @Override
        public void sync() throws IOException {
            channel.force(false);
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    private static final class RealReadableFile implements ReadableFile {
        private final FileChannel channel;

        RealReadableFile(FileChannel channel) {
            this.channel = channel;
        }

        @Override
        public int read(long position, ByteBuffer dst) throws IOException {
            int total = 0;
            while (dst.hasRemaining()) {
                int n = channel.read(dst, position + total);
                if (n < 0) {
                    break;
                }
                total += n;
            }
            return total;
        }

        @Override
        public long size() throws IOException {
            return channel.size();
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }
}
