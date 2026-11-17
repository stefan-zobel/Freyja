package io.github.stefan_zobel.freyja.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.github.stefan_zobel.freyja.lsm.fs.RealFileSystem;
import io.github.stefan_zobel.freyja.lsm.table.InternalKey;
import io.github.stefan_zobel.freyja.lsm.table.SSTableWriter;

/** Runs the table dump tool against files on the real file system. */
class SstDumpTest {

    @TempDir
    Path dir;

    private final RealFileSystem fs = new RealFileSystem();
    private final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errBytes = new ByteArrayOutputStream();

    private int run(String... args) {
        outBytes.reset();
        errBytes.reset();
        return SstDump.run(args, fs,
                new PrintStream(outBytes, true, StandardCharsets.UTF_8),
                new PrintStream(errBytes, true, StandardCharsets.UTF_8));
    }

    private String out() {
        return outBytes.toString(StandardCharsets.UTF_8);
    }

    private String err() {
        return errBytes.toString(StandardCharsets.UTF_8);
    }

    private Path writeTable(String name, long fileNumber) throws IOException {
        Path path = dir.resolve(name);
        try (SSTableWriter writer = new SSTableWriter(fs, path, fileNumber)) {
            writer.add(InternalKey.encode(new byte[] {0x61}, 9, InternalKey.TYPE_VALUE), new byte[] {0x01, 0x02});
            writer.add(InternalKey.encode(new byte[] {0x61}, 4, InternalKey.TYPE_TOMBSTONE), new byte[0]);
            writer.add(InternalKey.encode(new byte[] {0x62, 0x63}, 7, InternalKey.TYPE_VALUE), new byte[100]);
            writer.finish();
        }
        return path;
    }

    @Test
    void printsTheStructure() throws IOException {
        Path path = writeTable("000042.sst", 42);
        assertEquals(0, run(path.toString()), err());
        String text = out();
        assertTrue(text.contains("file number:     42"), text);
        assertTrue(text.contains("file size:       " + Files.size(path)), text);
        assertTrue(text.contains("format version:  1"), text);
        assertTrue(text.contains("1 buckets, 6 probes"), text);
        assertTrue(text.contains("data blocks:     1"), text);
        assertTrue(text.contains("block 0: offset 0,"), text);
        assertTrue(text.contains("3 entries, last key 6263 @7 VALUE"), text);
        assertTrue(text.contains("entries:         3"), text);
        assertFalse(text.contains("->"), "entries are printed only on request");
        assertEquals("", err());
    }

    @Test
    void printsEveryEntryOnRequest() throws IOException {
        Path path = writeTable("000042.sst", 42);
        assertEquals(0, run("--entries", path.toString()), err());
        String text = out();
        assertTrue(text.contains("61 @9 VALUE -> 0102 (2 bytes)"), text);
        assertTrue(text.contains("61 @4 TOMBSTONE ->  (0 bytes)"), text);
        assertTrue(text.contains("6263 @7 VALUE -> " + "00".repeat(32) + "... (100 bytes)"), text);
        assertTrue(text.indexOf("@9") < text.indexOf("@4"), "newer version first");
    }

    @Test
    void theFileNumberCanBeGivenForARenamedFile() throws IOException {
        Path path = writeTable("renamed.sst", 42);
        assertEquals(2, run(path.toString()));
        assertTrue(err().contains("--file-number"), err());

        assertEquals(0, run("--file-number", "42", path.toString()), err());
        assertTrue(out().contains("file number:     42"));
    }

    @Test
    void aWrongFileNumberOrDamageIsReportedAsAnError() throws IOException {
        Path path = writeTable("000042.sst", 42);
        assertEquals(1, run("--file-number", "43", path.toString()));
        assertTrue(err().contains("corruption"), err());

        byte[] bytes = Files.readAllBytes(path);
        bytes[5] ^= 0x10;
        Path damaged = dir.resolve("000043.sst");
        Files.write(damaged, bytes);
        assertEquals(1, run("--file-number", "42", damaged.toString()));
        assertTrue(err().contains("checksum mismatch"), err());
        assertTrue(err().contains("offset 0"), err());
    }

    @Test
    void usageErrors() {
        assertEquals(2, run());
        assertEquals(2, run("--unknown", "x.sst"));
        assertEquals(2, run("a.sst", "b.sst"));
        assertEquals(2, run("--file-number", "abc", "a.sst"));
        assertEquals(1, run(dir.resolve("000001.sst").toString())); // file does not exist
        assertTrue(err().startsWith("error:"), err());
    }

    @Test
    void fileNumbersAreTakenFromNames() {
        assertEquals(42L, SstDump.fileNumberFromName(Path.of("000042.sst")));
        assertEquals(0L, SstDump.fileNumberFromName(Path.of("dir", "000000.sst")));
        assertNull(SstDump.fileNumberFromName(Path.of("table.sst")));
        assertNull(SstDump.fileNumberFromName(Path.of(".sst")));
        assertNull(SstDump.fileNumberFromName(Path.of("12a.sst")));
    }
}
