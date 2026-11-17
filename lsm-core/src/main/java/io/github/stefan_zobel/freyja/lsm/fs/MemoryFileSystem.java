package io.github.stefan_zobel.freyja.lsm.fs;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A deterministic in-memory {@link StorageFileSystem} for tests and simulations.
 *
 * <p>It keeps two views of the world: the current state, and the durable state that would
 * survive a crash.
 * <ul>
 *   <li>File contents are durable up to the length at the last {@link WritableFile#sync()}.</li>
 *   <li>The names in a directory are durable as of the last {@link #syncDirectory(Path)}.</li>
 *   <li>{@link #crash()} replaces the current state by the durable state and invalidates
 *       every open handle. Bytes that were written but not synced are lost completely; finer
 *       outcomes (partial and garbled writes) are added by the fault-injecting wrapper.</li>
 *   <li>Directories themselves are durable as soon as they are created.</li>
 * </ul>
 *
 * <p>It enforces the handle rule on every platform: a name through which a file is open can
 * be neither deleted, nor renamed, nor replaced by a rename, nor truncated.
 *
 * <p>An optional capacity makes appends fail when the file system is full.
 */
public final class MemoryFileSystem implements StorageFileSystem {

    private static final class Inode {
        byte[] data = new byte[0];
        int size;
        int syncedSize;
    }

    private final Object lock = new Object();
    private final TreeSet<Path> directories = new TreeSet<>();
    private TreeMap<Path, Inode> entries = new TreeMap<>();
    private final TreeMap<Path, Inode> durableEntries = new TreeMap<>();
    private final Map<Path, Integer> openHandles = new HashMap<>();
    private final Set<Path> locks = new HashSet<>();
    private long capacity = Long.MAX_VALUE;
    private int epoch;

    // ---- test controls ---------------------------------------------------------------------

    /**
     * Simulates a crash: everything that is not durable disappears, and every handle that is
     * still open fails from now on.
     */
    public void crash() {
        synchronized (lock) {
            epoch++;
            openHandles.clear();
            locks.clear(); // a crashed process holds no locks
            entries = new TreeMap<>(durableEntries);
            for (Inode inode : entries.values()) {
                inode.size = inode.syncedSize;
            }
        }
    }

    /** Limits the total size of all files. Appends beyond it fail. */
    public void setCapacity(long bytes) {
        synchronized (lock) {
            capacity = bytes;
        }
    }

    /** Returns the number of handles that are currently open. */
    public int openHandleCount() {
        synchronized (lock) {
            int total = 0;
            for (int count : openHandles.values()) {
                total += count;
            }
            return total;
        }
    }

    // ---- StorageFileSystem -----------------------------------------------------------------

    @Override
    public void createDirectories(Path dir) throws IOException {
        synchronized (lock) {
            for (Path p = key(dir); p != null; p = p.getParent()) {
                if (entries.containsKey(p)) {
                    throw new FileAlreadyExistsException(p.toString());
                }
                directories.add(p);
            }
        }
    }

    @Override
    public WritableFile create(Path path) throws IOException {
        synchronized (lock) {
            Path name = key(path);
            requireParentDirectory(name);
            if (entries.containsKey(name) || directories.contains(name)) {
                throw new FileAlreadyExistsException(name.toString());
            }
            Inode inode = new Inode();
            entries.put(name, inode);
            return new Handle(name, inode);
        }
    }

    @Override
    public ReadableFile open(Path path) throws IOException {
        synchronized (lock) {
            Path name = key(path);
            return new Handle(name, requireFile(name));
        }
    }

    @Override
    public void rename(Path source, Path target) throws IOException {
        synchronized (lock) {
            Path from = key(source);
            Path to = key(target);
            Inode inode = requireFile(from);
            requireParentDirectory(to);
            requireNotOpen(from);
            requireNotOpen(to);
            if (directories.contains(to)) {
                throw new FileSystemException(to.toString(), null, "is a directory");
            }
            entries.remove(from);
            entries.put(to, inode);
        }
    }

    @Override
    public void link(Path existing, Path link) throws IOException {
        synchronized (lock) {
            Path from = key(existing);
            Path to = key(link);
            Inode inode = requireFile(from);
            requireParentDirectory(to);
            if (entries.containsKey(to) || directories.contains(to)) {
                throw new FileAlreadyExistsException(to.toString());
            }
            entries.put(to, inode);
        }
    }

    @Override
    public void delete(Path path) throws IOException {
        synchronized (lock) {
            Path name = key(path);
            requireFile(name);
            requireNotOpen(name);
            entries.remove(name);
        }
    }

    @Override
    public boolean exists(Path path) {
        synchronized (lock) {
            Path name = key(path);
            return entries.containsKey(name) || directories.contains(name);
        }
    }

    @Override
    public long size(Path path) throws IOException {
        synchronized (lock) {
            return requireFile(key(path)).size;
        }
    }

    @Override
    public void truncate(Path path, long size) throws IOException {
        synchronized (lock) {
            Path name = key(path);
            Inode inode = requireFile(name);
            requireNotOpen(name);
            if (size < 0 || size > inode.size) {
                throw new IllegalArgumentException(
                        "cannot truncate " + name + " of size " + inode.size + " to " + size);
            }
            inode.size = (int) size;
            inode.syncedSize = (int) size;
        }
    }

    @Override
    public List<Path> list(Path dir) throws IOException {
        synchronized (lock) {
            Path parent = key(dir);
            requireDirectory(parent);
            List<Path> result = new ArrayList<>();
            for (Path p : entries.keySet()) {
                if (parent.equals(p.getParent())) {
                    result.add(p);
                }
            }
            for (Path p : directories) {
                if (parent.equals(p.getParent())) {
                    result.add(p);
                }
            }
            Collections.sort(result);
            return result;
        }
    }

    @Override
    public void syncDirectory(Path dir) throws IOException {
        synchronized (lock) {
            Path parent = key(dir);
            requireDirectory(parent);
            durableEntries.keySet().removeIf(p -> parent.equals(p.getParent()));
            for (Map.Entry<Path, Inode> e : entries.entrySet()) {
                if (parent.equals(e.getKey().getParent())) {
                    durableEntries.put(e.getKey(), e.getValue());
                }
            }
        }
    }

    @Override
    public long freeSpace(Path dir) throws IOException {
        synchronized (lock) {
            requireDirectory(key(dir));
            return Math.max(0, capacity - usedBytes());
        }
    }

    @Override
    public Closeable lock(Path path) throws IOException {
        synchronized (lock) {
            Path name = key(path);
            if (!entries.containsKey(name)) {
                requireParentDirectory(name);
                if (directories.contains(name)) {
                    throw new FileSystemException(name.toString(), null, "is a directory");
                }
                entries.put(name, new Inode());
            }
            if (!locks.add(name)) {
                throw new IOException("the lock " + name + " is held by another owner");
            }
            int lockedInEpoch = epoch;
            return () -> {
                synchronized (lock) {
                    if (lockedInEpoch == epoch) {
                        locks.remove(name);
                    }
                }
            };
        }
    }

    // ---- internals -------------------------------------------------------------------------

    private static Path key(Path path) {
        return path.toAbsolutePath().normalize();
    }

    /** Total size of all files; a file with several names counts once. */
    private long usedBytes() {
        IdentityHashMap<Inode, Boolean> seen = new IdentityHashMap<>();
        long total = 0;
        for (Inode inode : entries.values()) {
            if (seen.put(inode, Boolean.TRUE) == null) {
                total += inode.size;
            }
        }
        return total;
    }

    private Inode requireFile(Path name) throws IOException {
        Inode inode = entries.get(name);
        if (inode == null) {
            throw new NoSuchFileException(name.toString());
        }
        return inode;
    }

    private void requireDirectory(Path dir) throws IOException {
        if (!directories.contains(dir)) {
            throw new NoSuchFileException(dir.toString());
        }
    }

    private void requireParentDirectory(Path name) throws IOException {
        Path parent = name.getParent();
        if (parent == null || !directories.contains(parent)) {
            throw new NoSuchFileException(name.toString(), null, "parent directory does not exist");
        }
    }

    private void requireNotOpen(Path name) throws IOException {
        if (openHandles.getOrDefault(name, 0) > 0) {
            throw new FileSystemException(name.toString(), null,
                    "the file is open; close every handle first");
        }
    }

    /** One handle type serves both interfaces; a handle is handed out as only one of them. */
    private final class Handle implements WritableFile, ReadableFile {
        private final Path name;
        private final Inode inode;
        private final int openedInEpoch;
        private boolean closed;

        Handle(Path name, Inode inode) {
            this.name = name;
            this.inode = inode;
            this.openedInEpoch = epoch;
            openHandles.merge(name, 1, Integer::sum);
        }

        private void requireUsable() throws IOException {
            if (closed) {
                throw new ClosedChannelException();
            }
            if (openedInEpoch != epoch) {
                throw new IOException("handle of " + name + " was lost in a crash");
            }
        }

        @Override
        public void append(ByteBuffer src) throws IOException {
            synchronized (lock) {
                requireUsable();
                int length = src.remaining();
                if (capacity != Long.MAX_VALUE && usedBytes() + length > capacity) {
                    throw new IOException("no space left on device: " + name);
                }
                long newSize = (long) inode.size + length;
                if (newSize > Integer.MAX_VALUE - 8) {
                    throw new IOException("file too large for the in-memory file system: " + name);
                }
                if (newSize > inode.data.length) {
                    long grown = Math.max(newSize, Math.min(Integer.MAX_VALUE - 8, inode.data.length * 2L));
                    inode.data = Arrays.copyOf(inode.data, (int) Math.max(grown, 64));
                }
                src.get(inode.data, inode.size, length);
                inode.size = (int) newSize;
            }
        }

        @Override
        public void sync() throws IOException {
            synchronized (lock) {
                requireUsable();
                inode.syncedSize = inode.size;
            }
        }

        @Override
        public int read(long position, ByteBuffer dst) throws IOException {
            synchronized (lock) {
                requireUsable();
                if (position < 0) {
                    throw new IllegalArgumentException("negative position: " + position);
                }
                if (position >= inode.size) {
                    return 0;
                }
                int n = (int) Math.min(dst.remaining(), inode.size - position);
                dst.put(inode.data, (int) position, n);
                return n;
            }
        }

        @Override
        public long size() {
            synchronized (lock) {
                return inode.size;
            }
        }

        @Override
        public void close() {
            synchronized (lock) {
                if (closed) {
                    return;
                }
                closed = true;
                if (openedInEpoch == epoch) {
                    openHandles.computeIfPresent(name, (k, count) -> count > 1 ? count - 1 : null);
                }
            }
        }
    }
}
