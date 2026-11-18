package io.github.sz.freyja.lsm.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Measures what the platform does when a file with an open {@link FileChannel} is deleted,
 * replaced, or linked (docs/spec/storage-engine.md, section 2).
 *
 * <p>The test prints one line per case. The engine does not depend on any of these outcomes:
 * it closes every handle before it deletes or replaces a file. The measurement documents what
 * a violation of that rule would cause on each platform.
 */
class PlatformFileBehaviorTest {

    @TempDir
    Path dir;

    @FunctionalInterface
    private interface Action {
        void run() throws IOException;
    }

    private static String outcome(Action action) {
        try {
            action.run();
            return "succeeds";
        } catch (IOException | RuntimeException e) {
            return "fails with " + e.getClass().getSimpleName();
        }
    }

    private static void write(Path path, String content) throws IOException {
        Files.write(path, content.getBytes(StandardCharsets.US_ASCII));
    }

    private static String readThrough(FileChannel channel) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(16);
            int n = channel.read(buffer, 0);
            return "\"" + new String(buffer.array(), 0, Math.max(n, 0), StandardCharsets.US_ASCII) + "\"";
        } catch (IOException e) {
            return e.getClass().getSimpleName();
        }
    }

    private static void report(String label, String result) {
        System.out.println("[platform-file-behavior] " + System.getProperty("os.name")
                + " | " + label + ": " + result);
    }

    @Test
    void measure() throws IOException {
        // Case 1: delete a file that has an open channel.
        Path deleted = dir.resolve("deleted.dat");
        write(deleted, "one");
        try (FileChannel channel = FileChannel.open(deleted, StandardOpenOption.READ)) {
            String delete = outcome(() -> Files.delete(deleted));
            report("delete with open channel", delete
                    + "; name still listed: " + Files.exists(deleted)
                    + "; read through old channel: " + readThrough(channel));

            // Case 3: reuse the name while the old handle is still open.
            report("create same name while old channel open",
                    outcome(() -> Files.write(deleted, new byte[] {'n'}, StandardOpenOption.CREATE_NEW)));
        }
        report("create same name after closing the channel",
                outcome(() -> {
                    Files.deleteIfExists(deleted);
                    Files.write(deleted, new byte[] {'n'}, StandardOpenOption.CREATE_NEW);
                }));

        // Case 2: rename another file over a file that has an open channel.
        Path target = dir.resolve("target.dat");
        Path replacement = dir.resolve("replacement.tmp");
        write(target, "old");
        write(replacement, "new");
        try (FileChannel channel = FileChannel.open(target, StandardOpenOption.READ)) {
            String rename = outcome(() -> Files.move(replacement, target,
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING));
            report("rename over file with open channel", rename
                    + "; read through old channel: " + readThrough(channel)
                    + "; read by name: \"" + new String(Files.readAllBytes(target), StandardCharsets.US_ASCII) + "\"");
        }

        // Case 2b: rename a file that itself has an open channel.
        Path moving = dir.resolve("moving.dat");
        write(moving, "mov");
        try (FileChannel channel = FileChannel.open(moving, StandardOpenOption.READ)) {
            report("rename a file with open channel",
                    outcome(() -> Files.move(moving, dir.resolve("moved.dat"), StandardCopyOption.ATOMIC_MOVE))
                    + "; read through old channel: " + readThrough(channel));
        }

        // Case 4: hard-link a file that has an open channel, then delete the link.
        Path linked = dir.resolve("linked.dat");
        Path link = dir.resolve("link.dat");
        write(linked, "lnk");
        try (FileChannel channel = FileChannel.open(linked, StandardOpenOption.READ)) {
            report("hard link to file with open channel",
                    outcome(() -> Files.createLink(link, linked)));
            report("delete the link while the original is open",
                    outcome(() -> Files.delete(link))
                    + "; read through old channel: " + readThrough(channel));
        }
    }
}
