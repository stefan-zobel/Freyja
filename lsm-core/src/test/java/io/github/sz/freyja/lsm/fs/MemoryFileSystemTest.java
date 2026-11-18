package io.github.sz.freyja.lsm.fs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Runs the file-system contract against the in-memory file system and checks what only it
 * offers: the crash model, the handle rule, and the capacity limit.
 */
class MemoryFileSystemTest extends FileSystemContract {

    private final MemoryFileSystem fs = new MemoryFileSystem();
    private final Path root = Path.of("memory-root").toAbsolutePath();

    @BeforeEach
    void createRoot() throws IOException {
        fs.createDirectories(root);
    }

    @Override
    StorageFileSystem fs() {
        return fs;
    }

    @Override
    Path root() {
        return root;
    }

    private void write(Path path, String content, boolean sync) throws IOException {
        try (WritableFile file = fs.create(path)) {
            byte[] bytes = content.getBytes(StandardCharsets.US_ASCII);
            file.append(bytes, 0, bytes.length);
            if (sync) {
                file.sync();
            }
        }
    }

    private String read(Path path) throws IOException {
        try (ReadableFile file = fs.open(path)) {
            byte[] bytes = new byte[(int) file.size()];
            file.readFully(0, bytes, 0, bytes.length);
            return new String(bytes, StandardCharsets.US_ASCII);
        }
    }

    // ---- crash model -----------------------------------------------------------------------

    @Test
    void aCrashKeepsSyncedBytesAndDropsTheRest() throws IOException {
        Path path = root.resolve("a.wal");
        WritableFile file = fs.create(path);
        file.append("synced".getBytes(StandardCharsets.US_ASCII), 0, 6);
        file.sync();
        file.append("-lost".getBytes(StandardCharsets.US_ASCII), 0, 5);
        fs.syncDirectory(root);

        fs.crash();

        assertEquals("synced", read(path));
        // The handle from before the crash is dead.
        assertThrows(IOException.class, () -> file.append(new byte[1], 0, 1));
        assertThrows(IOException.class, file::sync);
        file.close();
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void aCrashRemovesAFileWhoseNameWasNotSynced() throws IOException {
        Path durable = root.resolve("durable.sst");
        Path fresh = root.resolve("fresh.sst");
        write(durable, "d", true);
        fs.syncDirectory(root);
        write(fresh, "f", true); // contents synced, name not

        fs.crash();

        assertEquals("d", read(durable));
        assertFalse(fs.exists(fresh));
    }

    @Test
    void aCrashUndoesARenameThatWasNotSynced() throws IOException {
        Path current = root.resolve("CURRENT");
        Path tmp = root.resolve("CURRENT.tmp");
        write(current, "old", true);
        fs.syncDirectory(root);
        write(tmp, "new", true);
        fs.rename(tmp, current);
        assertEquals("new", read(current));

        fs.crash();
        assertEquals("old", read(current));
        assertFalse(fs.exists(tmp));
    }

    @Test
    void aSyncedRenameSurvivesACrash() throws IOException {
        Path current = root.resolve("CURRENT");
        Path tmp = root.resolve("CURRENT.tmp");
        write(current, "old", true);
        fs.syncDirectory(root);
        write(tmp, "new", true);
        fs.rename(tmp, current);
        fs.syncDirectory(root);

        fs.crash();
        assertEquals("new", read(current));
        assertFalse(fs.exists(tmp));
    }

    @Test
    void aCrashUndoesADeleteAndALinkThatWereNotSynced() throws IOException {
        Path a = root.resolve("a.sst");
        Path link = root.resolve("link.sst");
        write(a, "data", true);
        fs.syncDirectory(root);
        fs.link(a, link);
        fs.delete(a);
        assertFalse(fs.exists(a));
        assertTrue(fs.exists(link));

        fs.crash();
        assertEquals("data", read(a));
        assertFalse(fs.exists(link));
    }

    @Test
    void directorySyncCoversOnlyThatDirectory() throws IOException {
        Path sub = root.resolve("sub");
        fs.createDirectories(sub);
        write(root.resolve("top"), "t", true);
        write(sub.resolve("inner"), "i", true);
        fs.syncDirectory(sub);

        fs.crash();
        assertFalse(fs.exists(root.resolve("top")));
        assertEquals("i", read(sub.resolve("inner")));
    }

    @Test
    void truncationIsDurable() throws IOException {
        Path path = root.resolve("a.wal");
        write(path, "0123456789", true);
        fs.syncDirectory(root);
        fs.truncate(path, 3);
        fs.crash();
        assertEquals("012", read(path));
    }

    // ---- handle rule -----------------------------------------------------------------------

    @Test
    void anOpenFileCanNeitherBeDeletedNorRenamedNorReplacedNorTruncated() throws IOException {
        Path open = root.resolve("open.sst");
        Path other = root.resolve("other.sst");
        write(open, "o", true);
        write(other, "x", true);

        try (ReadableFile handle = fs.open(open)) {
            assertEquals(1, fs.openHandleCount());
            assertThrows(FileSystemException.class, () -> fs.delete(open));
            assertThrows(FileSystemException.class, () -> fs.rename(open, root.resolve("moved")));
            assertThrows(FileSystemException.class, () -> fs.rename(other, open));
            assertThrows(FileSystemException.class, () -> fs.truncate(open, 0));
            assertEquals(1, handle.read(0, ByteBuffer.allocate(4)));
        }
        assertEquals(0, fs.openHandleCount());
        fs.rename(other, open);
        fs.delete(open);
    }

    @Test
    void anOpenFileCanBeHardLinkedAndTheLinkDeleted() throws IOException {
        Path live = root.resolve("000003.sst");
        Path checkpoint = root.resolve("checkpoint");
        fs.createDirectories(checkpoint);
        write(live, "table", true);
        try (ReadableFile handle = fs.open(live)) {
            Path link = checkpoint.resolve("000003.sst");
            fs.link(live, link);
            assertEquals("table", read(link));
            fs.delete(link); // the file is open through the other name only
            assertEquals(5, handle.size());
        }
    }

    // ---- capacity --------------------------------------------------------------------------

    @Test
    void appendsFailWhenTheFileSystemIsFull() throws IOException {
        fs.setCapacity(100);
        assertEquals(100, fs.freeSpace(root));
        Path a = root.resolve("a");
        try (WritableFile file = fs.create(a)) {
            file.append(new byte[60], 0, 60);
            assertEquals(40, fs.freeSpace(root));
            // As on a real device, the part that still fits is written before the failure.
            assertThrows(NoSpaceException.class, () -> file.append(new byte[41], 0, 41));
            assertEquals(100, file.size());
            assertEquals(0, fs.freeSpace(root));
            assertThrows(NoSpaceException.class, () -> file.append(new byte[1], 0, 1));
        }
        // A hard link uses no additional space; deleting the last name frees it.
        fs.link(a, root.resolve("b"));
        assertEquals(0, fs.freeSpace(root));
        fs.delete(a);
        assertEquals(0, fs.freeSpace(root));
        fs.delete(root.resolve("b"));
        assertEquals(100, fs.freeSpace(root));
    }

    // ---- structure -------------------------------------------------------------------------

    @Test
    void filesNeedAnExistingParentDirectory() {
        assertThrows(NoSuchFileException.class, () -> fs.create(root.resolve("nowhere").resolve("f")));
        assertThrows(NoSuchFileException.class, () -> fs.list(root.resolve("nowhere")));
        assertThrows(NoSuchFileException.class, () -> fs.syncDirectory(root.resolve("nowhere")));
    }

    @Test
    void pathsAreNormalized() throws IOException {
        write(root.resolve("a"), "x", true);
        assertEquals("x", read(root.resolve("sub").resolve("..").resolve("a")));
    }
}
