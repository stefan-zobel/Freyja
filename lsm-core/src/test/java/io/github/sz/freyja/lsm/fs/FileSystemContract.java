package io.github.sz.freyja.lsm.fs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Behavior that every {@link StorageFileSystem} implementation must show. Each
 * implementation has a subclass that supplies the file system and a root directory.
 *
 * <p>Every test closes its handles before it deletes or renames, as the interface requires.
 */
abstract class FileSystemContract {

    /** Returns the file system under test. */
    abstract StorageFileSystem fs();

    /** Returns an existing, empty directory. */
    abstract Path root();

    private static byte[] ascii(String text) {
        return text.getBytes(StandardCharsets.US_ASCII);
    }

    private void write(Path path, String content, boolean sync) throws IOException {
        try (WritableFile file = fs().create(path)) {
            byte[] bytes = ascii(content);
            file.append(bytes, 0, bytes.length);
            if (sync) {
                file.sync();
            }
        }
    }

    private String read(Path path) throws IOException {
        try (ReadableFile file = fs().open(path)) {
            byte[] bytes = new byte[(int) file.size()];
            file.readFully(0, bytes, 0, bytes.length);
            return new String(bytes, StandardCharsets.US_ASCII);
        }
    }

    @Test
    void appendedBytesAreReadBack() throws IOException {
        Path path = root().resolve("a.dat");
        try (WritableFile file = fs().create(path)) {
            assertEquals(0, file.size());
            file.append(ascii("hello "), 0, 6);
            file.append(ByteBuffer.wrap(ascii("xxworldxx"), 2, 5));
            assertEquals(11, file.size());
            file.sync();
        }
        assertEquals(11, fs().size(path));
        assertEquals("hello world", read(path));
    }

    @Test
    void positionalReadsStopAtTheEndOfTheFile() throws IOException {
        Path path = root().resolve("a.dat");
        write(path, "0123456789", true);
        try (ReadableFile file = fs().open(path)) {
            assertEquals(10, file.size());

            byte[] middle = new byte[4];
            assertEquals(4, file.read(3, ByteBuffer.wrap(middle)));
            assertArrayEquals(ascii("3456"), middle);

            byte[] tail = new byte[8];
            assertEquals(3, file.read(7, ByteBuffer.wrap(tail)));
            assertArrayEquals(ascii("789"), java.util.Arrays.copyOf(tail, 3));

            assertEquals(0, file.read(10, ByteBuffer.wrap(new byte[4])));
            assertEquals(0, file.read(500, ByteBuffer.wrap(new byte[4])));
            assertThrows(EOFException.class, () -> file.readFully(8, new byte[4], 0, 4));
        }
    }

    @Test
    void aLargeFileIsWrittenAndReadInPieces() throws IOException {
        Path path = root().resolve("big.dat");
        byte[] chunk = new byte[10_007];
        for (int i = 0; i < chunk.length; i++) {
            chunk[i] = (byte) (i * 31);
        }
        try (WritableFile file = fs().create(path)) {
            for (int i = 0; i < 50; i++) {
                file.append(chunk, 0, chunk.length);
            }
            file.sync();
        }
        try (ReadableFile file = fs().open(path)) {
            assertEquals(50L * chunk.length, file.size());
            byte[] back = new byte[chunk.length];
            file.readFully(49L * chunk.length, back, 0, back.length);
            assertArrayEquals(chunk, back);
        }
    }

    @Test
    void createFailsIfTheFileExists() throws IOException {
        Path path = root().resolve("a.dat");
        write(path, "x", false);
        assertThrows(FileAlreadyExistsException.class, () -> fs().create(path));
        assertEquals("x", read(path));
    }

    @Test
    void openFailsIfTheFileDoesNotExist() {
        assertThrows(NoSuchFileException.class, () -> fs().open(root().resolve("missing")));
        assertThrows(NoSuchFileException.class, () -> fs().size(root().resolve("missing")));
        assertThrows(NoSuchFileException.class, () -> fs().delete(root().resolve("missing")));
    }

    @Test
    void renameMovesAndReplaces() throws IOException {
        Path a = root().resolve("a.tmp");
        Path b = root().resolve("b.dat");
        write(a, "new", true);
        write(b, "old", true);
        fs().rename(a, b);
        fs().syncDirectory(root());
        assertFalse(fs().exists(a));
        assertEquals("new", read(b));

        Path c = root().resolve("c.dat");
        fs().rename(b, c);
        assertFalse(fs().exists(b));
        assertEquals("new", read(c));
    }

    @Test
    void aHardLinkIsASecondNameForTheSameFile() throws IOException {
        fs().createDirectories(root().resolve("copy"));
        Path original = root().resolve("000007.sst");
        Path link = root().resolve("copy").resolve("000007.sst");
        write(original, "table", true);
        fs().link(original, link);
        assertEquals("table", read(link));
        assertThrows(FileAlreadyExistsException.class, () -> fs().link(original, link));

        // Deleting one name keeps the file reachable through the other.
        fs().delete(original);
        assertFalse(fs().exists(original));
        assertEquals("table", read(link));
    }

    @Test
    void deleteRemovesTheName() throws IOException {
        Path path = root().resolve("a.dat");
        write(path, "x", true);
        assertTrue(fs().exists(path));
        fs().delete(path);
        assertFalse(fs().exists(path));
        // The name can be reused.
        write(path, "y", true);
        assertEquals("y", read(path));
    }

    @Test
    void truncateCutsTheFile() throws IOException {
        Path path = root().resolve("a.wal");
        write(path, "0123456789", true);
        fs().truncate(path, 4);
        assertEquals(4, fs().size(path));
        assertEquals("0123", read(path));
        fs().truncate(path, 4);
        assertEquals("0123", read(path));
        assertThrows(IllegalArgumentException.class, () -> fs().truncate(path, 5));
        fs().truncate(path, 0);
        assertEquals(0, fs().size(path));
    }

    @Test
    void listReturnsSortedFullPaths() throws IOException {
        fs().createDirectories(root().resolve("sub").resolve("deeper"));
        write(root().resolve("b.sst"), "", false);
        write(root().resolve("a.wal"), "", false);
        write(root().resolve("sub").resolve("inner.sst"), "", false);

        assertEquals(List.of(root().resolve("a.wal"), root().resolve("b.sst"), root().resolve("sub")),
                fs().list(root()));
        assertEquals(List.of(root().resolve("sub").resolve("deeper"),
                root().resolve("sub").resolve("inner.sst")), fs().list(root().resolve("sub")));
        assertEquals(List.of(), fs().list(root().resolve("sub").resolve("deeper")));
    }

    @Test
    void createDirectoriesIsIdempotent() throws IOException {
        Path dir = root().resolve("x").resolve("y");
        fs().createDirectories(dir);
        fs().createDirectories(dir);
        assertTrue(fs().exists(dir));
        fs().syncDirectory(dir);
        write(dir.resolve("f"), "ok", true);
        assertEquals("ok", read(dir.resolve("f")));
    }

    @Test
    void anEmptyDirectoryIsRemoved() throws IOException {
        Path dir = root().resolve("gone");
        fs().createDirectories(dir);
        write(dir.resolve("file"), "x", true);
        assertThrows(java.nio.file.DirectoryNotEmptyException.class, () -> fs().deleteDirectory(dir));
        fs().delete(dir.resolve("file"));
        fs().deleteDirectory(dir);
        assertFalse(fs().exists(dir));
        assertThrows(java.nio.file.NoSuchFileException.class, () -> fs().deleteDirectory(dir));
        assertThrows(java.nio.file.NoSuchFileException.class, () -> fs().create(dir.resolve("file")).close());
        // The name is free again.
        fs().createDirectories(dir);
        write(dir.resolve("file"), "y", true);
    }

    @Test
    void freeSpaceIsReported() throws IOException {
        assertTrue(fs().freeSpace(root()) > 0);
    }

    @Test
    void aLockIsExclusiveUntilItIsReleased() throws IOException {
        Path lockFile = root().resolve("LOCK");
        java.io.Closeable first = fs().lock(lockFile);
        assertTrue(fs().exists(lockFile));
        assertThrows(IOException.class, () -> fs().lock(lockFile));
        first.close();
        first.close();
        java.io.Closeable second = fs().lock(lockFile);
        assertThrows(IOException.class, () -> fs().lock(lockFile));
        second.close();
        // Another lock file is independent.
        try (java.io.Closeable other = fs().lock(root().resolve("OTHER"))) {
            fs().lock(lockFile).close();
        }
    }

    @Test
    void closingTwiceIsHarmless() throws IOException {
        Path path = root().resolve("a.dat");
        WritableFile writable = fs().create(path);
        writable.close();
        writable.close();
        ReadableFile readable = fs().open(path);
        readable.close();
        readable.close();
        assertThrows(IOException.class, () -> readable.read(0, ByteBuffer.allocate(1)));
        assertThrows(IOException.class, () -> writable.append(new byte[1], 0, 1));
    }
}
