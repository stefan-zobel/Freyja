package io.github.sz.freyja.lsm.fs;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

/**
 * A {@link StorageFileSystem} that wraps another one and injects faults
 * (docs/spec/verification.md, section 2.2).
 *
 * <p>Every operation that changes something is counted. Two kinds of faults can be set up:
 * <ul>
 *   <li><b>Failures.</b> A {@link Fault} is asked before every counted operation and may
 *       answer with an {@link IOException}, which the operation then throws without doing
 *       anything.</li>
 *   <li><b>A crash.</b> At a chosen operation the process "dies": the operation is carried
 *       out not at all, in part (an append), or completely, the crash action runs, and
 *       {@link SimulatedCrash} is thrown. From then on every call on this file system and on
 *       its handles throws {@code SimulatedCrash} too, so that code which keeps running
 *       after the "death" cannot change what the crash left behind.</li>
 * </ul>
 *
 * <p>Safe for concurrent use; the operation count is exact only without concurrency.
 */
public final class FaultyFileSystem implements StorageFileSystem {

    /** The operations that are counted and can fail. */
    public enum Operation {
        CREATE_DIRECTORIES, CREATE, APPEND, SYNC, RENAME, LINK, DELETE, TRUNCATE, SYNC_DIRECTORY, LOCK
    }

    /** Decides whether an operation fails. */
    @FunctionalInterface
    public interface Fault {

        /**
         * @param operation the operation that is about to run
         * @param path      the file or directory it concerns
         * @param index     the number of counted operations before this one
         * @return the failure to throw instead of running the operation, or {@code null}
         */
        IOException check(Operation operation, Path path, long index);
    }

    /** Thrown at and after a simulated crash. It is an {@link Error} so that no handler for
     * ordinary failures swallows it. */
    public static final class SimulatedCrash extends Error {
        private static final long serialVersionUID = 1L;

        SimulatedCrash(String message) {
            super(message, null, false, false);
        }
    }

    private final StorageFileSystem delegate;
    private final Object monitor = new Object();
    private long operations;
    private Fault fault;
    private long crashAt = -1;
    private Random crashRandom;
    private Runnable crashAction;
    private boolean crashed;

    public FaultyFileSystem(StorageFileSystem delegate) {
        this.delegate = delegate;
    }

    /** Sets the source of failures, or removes it with {@code null}. */
    public void setFault(Fault fault) {
        synchronized (monitor) {
            this.fault = fault;
        }
    }

    /**
     * Schedules a crash at the counted operation with this index (0 is the next one if no
     * operation has run yet).
     *
     * @param random decides how much of the operation happens before the crash
     * @param action runs at the moment of the crash, typically
     *               {@link MemoryFileSystem#crash(Random)} of the wrapped file system
     */
    public void crashAt(long operationIndex, Random random, Runnable action) {
        synchronized (monitor) {
            this.crashAt = operationIndex;
            this.crashRandom = random;
            this.crashAction = action;
        }
    }

    /** Returns the number of counted operations so far. */
    public long operationCount() {
        synchronized (monitor) {
            return operations;
        }
    }

    /** Returns whether the simulated crash has happened. */
    public boolean crashed() {
        synchronized (monitor) {
            return crashed;
        }
    }

    /** How an operation proceeds. */
    private enum Outcome { RUN, CRASH_BEFORE, CRASH_DURING, CRASH_AFTER }

    /**
     * Counts one operation and decides its outcome. Throws the injected failure, if any.
     * The caller holds no lock while the operation itself runs.
     */
    private Outcome begin(Operation operation, Path path) throws IOException {
        synchronized (monitor) {
            requireAlive();
            long index = operations++;
            if (index == crashAt) {
                return switch (crashRandom.nextInt(3)) {
                    case 0 -> Outcome.CRASH_BEFORE;
                    case 1 -> Outcome.CRASH_DURING;
                    default -> Outcome.CRASH_AFTER;
                };
            }
            if (fault != null) {
                IOException failure = fault.check(operation, path, index);
                if (failure != null) {
                    throw failure;
                }
            }
            return Outcome.RUN;
        }
    }

    private void requireAlive() {
        if (crashed) {
            throw new SimulatedCrash("the process has crashed");
        }
    }

    private void checkAlive() {
        synchronized (monitor) {
            requireAlive();
        }
    }

    /** Runs the crash action and throws. */
    private SimulatedCrash crash(Operation operation, Path path) {
        Runnable action;
        synchronized (monitor) {
            crashed = true;
            action = crashAction;
        }
        if (action != null) {
            action.run();
        }
        return new SimulatedCrash("crash at " + operation + " of " + path);
    }

    @FunctionalInterface
    private interface Step {
        void run() throws IOException;
    }

    /** Runs a counted operation that happens completely or not at all. */
    private void atomic(Operation operation, Path path, Step step) throws IOException {
        Outcome outcome = begin(operation, path);
        if (outcome == Outcome.CRASH_BEFORE || outcome == Outcome.CRASH_DURING) {
            throw crash(operation, path);
        }
        step.run();
        if (outcome == Outcome.CRASH_AFTER) {
            throw crash(operation, path);
        }
    }

    // ---- StorageFileSystem -----------------------------------------------------------------

    @Override
    public void createDirectories(Path dir) throws IOException {
        atomic(Operation.CREATE_DIRECTORIES, dir, () -> delegate.createDirectories(dir));
    }

    @Override
    public WritableFile create(Path path) throws IOException {
        WritableFile[] file = new WritableFile[1];
        atomic(Operation.CREATE, path, () -> file[0] = delegate.create(path));
        return new FaultyWritableFile(path, file[0]);
    }

    @Override
    public ReadableFile open(Path path) throws IOException {
        checkAlive();
        ReadableFile file = delegate.open(path);
        return new ReadableFile() {
            @Override
            public int read(long position, ByteBuffer dst) throws IOException {
                checkAlive();
                return file.read(position, dst);
            }

            @Override
            public long size() throws IOException {
                checkAlive();
                return file.size();
            }

            @Override
            public void close() throws IOException {
                file.close(); // closing is always allowed, also after the crash
            }
        };
    }

    @Override
    public void rename(Path source, Path target) throws IOException {
        atomic(Operation.RENAME, target, () -> delegate.rename(source, target));
    }

    @Override
    public void link(Path existing, Path link) throws IOException {
        atomic(Operation.LINK, link, () -> delegate.link(existing, link));
    }

    @Override
    public void delete(Path path) throws IOException {
        atomic(Operation.DELETE, path, () -> delegate.delete(path));
    }

    @Override
    public void deleteDirectory(Path dir) throws IOException {
        atomic(Operation.DELETE, dir, () -> delegate.deleteDirectory(dir));
    }

    @Override
    public boolean exists(Path path) throws IOException {
        checkAlive();
        return delegate.exists(path);
    }

    @Override
    public long size(Path path) throws IOException {
        checkAlive();
        return delegate.size(path);
    }

    @Override
    public void truncate(Path path, long size) throws IOException {
        atomic(Operation.TRUNCATE, path, () -> delegate.truncate(path, size));
    }

    @Override
    public List<Path> list(Path dir) throws IOException {
        checkAlive();
        return delegate.list(dir);
    }

    @Override
    public void syncDirectory(Path dir) throws IOException {
        atomic(Operation.SYNC_DIRECTORY, dir, () -> delegate.syncDirectory(dir));
    }

    @Override
    public long freeSpace(Path dir) throws IOException {
        checkAlive();
        return delegate.freeSpace(dir);
    }

    @Override
    public Closeable lock(Path path) throws IOException {
        Closeable[] handle = new Closeable[1];
        atomic(Operation.LOCK, path, () -> handle[0] = delegate.lock(path));
        return handle[0];
    }

    private final class FaultyWritableFile implements WritableFile {
        private final Path path;
        private final WritableFile file;

        FaultyWritableFile(Path path, WritableFile file) {
            this.path = path;
            this.file = file;
        }

        @Override
        public void append(ByteBuffer src) throws IOException {
            Outcome outcome = begin(Operation.APPEND, path);
            if (outcome == Outcome.CRASH_BEFORE) {
                throw crash(Operation.APPEND, path);
            }
            if (outcome == Outcome.CRASH_DURING) {
                // Only the first part of the data reaches the file.
                int part;
                synchronized (monitor) {
                    part = crashRandom.nextInt(src.remaining() + 1);
                }
                ByteBuffer prefix = src.duplicate();
                prefix.limit(prefix.position() + part);
                file.append(prefix);
                throw crash(Operation.APPEND, path);
            }
            file.append(src);
            if (outcome == Outcome.CRASH_AFTER) {
                throw crash(Operation.APPEND, path);
            }
        }

        @Override
        public void sync() throws IOException {
            atomic(Operation.SYNC, path, file::sync);
        }

        @Override
        public long size() {
            return file.size();
        }

        @Override
        public void close() throws IOException {
            file.close(); // closing is always allowed, also after the crash
        }
    }
}
