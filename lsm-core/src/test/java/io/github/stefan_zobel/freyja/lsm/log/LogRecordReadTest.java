package io.github.stefan_zobel.freyja.lsm.log;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.lsm.CorruptionException;
import io.github.stefan_zobel.freyja.lsm.fs.MemoryFileSystem;
import io.github.stefan_zobel.freyja.lsm.fs.ReadableFile;
import io.github.stefan_zobel.freyja.lsm.fs.WritableFile;

/** Reading one record at a known position (the Raft log store reads entries this way). */
class LogRecordReadTest {

    private static final Path DIR = Path.of("logs");
    private static final Path FILE = DIR.resolve("log");
    private static final long NUMBER = 17;

    @Test
    void everyRecordCanBeReadFromItsFragmentOffsetAndFromTheSizeBeforeItWasAdded() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        fs.createDirectories(DIR);
        Random random = new Random(5);
        List<byte[]> payloads = new ArrayList<>();
        List<Long> sizesBefore = new ArrayList<>();
        try (WritableFile file = fs.create(FILE)) {
            LogWriter writer = new LogWriter(file, NUMBER, FileKind.RAFT_LOG);
            for (int i = 0; i < 400; i++) {
                // Small records, records around a block, and records over several blocks.
                int length = switch (i % 4) {
                    case 0 -> random.nextInt(50);
                    case 1 -> random.nextInt(3000);
                    case 2 -> LogFormat.BLOCK_SIZE - 20 + random.nextInt(40);
                    default -> random.nextInt(3 * LogFormat.BLOCK_SIZE);
                };
                byte[] payload = new byte[length];
                random.nextBytes(payload);
                sizesBefore.add(writer.size());
                writer.addRecord(payload);
                payloads.add(payload);
                if (random.nextInt(3) == 0) {
                    writer.sync(); // a sync point goes before the next fragment
                }
            }
            writer.sync();
        }
        List<Long> fragmentOffsets = new ArrayList<>();
        try (ReadableFile file = fs.open(FILE)) {
            LogReader.read(file, "log", NUMBER, FileKind.RAFT_LOG, false,
                    (offset, payload) -> fragmentOffsets.add(offset));
            assertEquals(payloads.size(), fragmentOffsets.size());
            boolean differ = false;
            for (int i = 0; i < payloads.size(); i++) {
                assertArrayEquals(payloads.get(i), LogReader.readRecord(file, "log", NUMBER, fragmentOffsets.get(i)));
                assertArrayEquals(payloads.get(i), LogReader.readRecord(file, "log", NUMBER, sizesBefore.get(i)));
                differ |= !fragmentOffsets.get(i).equals(sizesBefore.get(i));
            }
            assertTrue(differ, "padding and sync points lie before some records");
        }
        assertEquals(0, fs.openHandleCount());
    }

    @Test
    void damageAndWrongPositionsAreReported() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        fs.createDirectories(DIR);
        byte[] big = new byte[2 * LogFormat.BLOCK_SIZE];
        new Random(1).nextBytes(big);
        long second;
        long third;
        try (WritableFile file = fs.create(FILE)) {
            LogWriter writer = new LogWriter(file, NUMBER, FileKind.RAFT_LOG);
            writer.addRecord(new byte[] {1, 2, 3});
            second = writer.size();
            writer.addRecord(big);
            third = writer.size();
            writer.addRecord(new byte[] {4});
            writer.sync();
        }
        try (ReadableFile file = fs.open(FILE)) {
            assertArrayEquals(big, LogReader.readRecord(file, "log", NUMBER, second));
            // The record was written under another file number.
            assertThrows(CorruptionException.class, () -> LogReader.readRecord(file, "log", NUMBER + 1, second));
            // A position inside a record: the checksum is bound to the offset.
            assertThrows(CorruptionException.class, () -> LogReader.readRecord(file, "log", NUMBER, second + 1));
            // A position at a later fragment of the record: no record starts there.
            long middle = LogFormat.BLOCK_SIZE;
            assertThrows(CorruptionException.class, () -> LogReader.readRecord(file, "log", NUMBER, middle));
            assertThrows(CorruptionException.class, () -> LogReader.readRecord(file, "log", NUMBER, file.size()));
            assertArrayEquals(new byte[] {4}, LogReader.readRecord(file, "log", NUMBER, third));
        }

        // Every flipped bit of the large record is noticed.
        byte[] bytes;
        try (ReadableFile file = fs.open(FILE)) {
            bytes = new byte[(int) file.size()];
            file.readFully(0, bytes, 0, bytes.length);
        }
        Random random = new Random(2);
        for (int i = 0; i < 300; i++) {
            int at = (int) second + random.nextInt((int) (third - second));
            byte[] damaged = bytes.clone();
            damaged[at] ^= (byte) (1 << random.nextInt(8));
            Path copy = DIR.resolve("damaged-" + i);
            try (WritableFile file = fs.create(copy)) {
                file.append(ByteBuffer.wrap(damaged));
            }
            try (ReadableFile file = fs.open(copy)) {
                assertThrows(CorruptionException.class, () -> LogReader.readRecord(file, "log", NUMBER, second),
                        "a flipped bit at " + at);
            }
        }
    }
}
