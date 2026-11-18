package io.github.stefan_zobel.freyja.node.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The token file of the listener for clients (docs/spec/operations.md, section 5). */
class TokenFileTest {

    private static final String ONE = "token-number-one-of-the-test";
    private static final String TWO = "token-number-two-of-the-test";

    @TempDir
    Path directory;

    @Test
    void aTokenOfTheFileIsAcceptedAndNothingElse() throws IOException {
        Path file = directory.resolve("tokens");
        Files.writeString(file, "# issued to the reporting job\r\n" + ONE + "\r\n\r\n   " + TWO + "  \n# " + ONE + "x\n");
        TokenFile tokens = TokenFile.open(file, 0);
        assertTrue(tokens.accepts(ONE));
        assertTrue(tokens.accepts(TWO));
        assertFalse(tokens.accepts(""));
        assertFalse(tokens.accepts(ONE + "x"));
        assertFalse(tokens.accepts(ONE.substring(1)));
        assertFalse(tokens.accepts("# issued to the reporting job"));
    }

    @Test
    void aChangedFileIsReadAgainAndABrokenOneIsNotUsed() throws IOException {
        Path file = directory.resolve("tokens");
        Files.writeString(file, ONE + "\n");
        TokenFile tokens = TokenFile.open(file, 0);
        assertTrue(tokens.accepts(ONE));

        Files.writeString(file, TWO + "\n");
        assertTrue(tokens.accepts(TWO));
        assertFalse(tokens.accepts(ONE));
        assertEquals(1, tokens.reloadCount());

        Files.writeString(file, "short\n");
        assertTrue(tokens.accepts(TWO), "the last good tokens stay in force");
        assertFalse(tokens.accepts("short"));
        Files.delete(file);
        assertTrue(tokens.accepts(TWO));
        assertTrue(tokens.reloadFailureCount() >= 2);
        assertEquals(1, tokens.reloadCount());

        Files.writeString(file, ONE + "\n");
        assertTrue(tokens.accepts(ONE));
        assertFalse(tokens.accepts(TWO));
        assertEquals(2, tokens.reloadCount());
    }

    @Test
    void aFileWithoutAUsableTokenIsRefusedAtTheStart() throws IOException {
        Path file = directory.resolve("tokens");
        assertThrows(IOException.class, () -> TokenFile.open(file));
        Files.writeString(file, "# nothing\n\n");
        assertThrows(IOException.class, () -> TokenFile.open(file));
        Files.writeString(file, ONE + "\nshort\n");
        assertThrows(IOException.class, () -> TokenFile.open(file));
        Files.writeString(file, "x".repeat(TokenFile.MAX_BYTES + 1) + "\n");
        assertThrows(IOException.class, () -> TokenFile.open(file));
        Files.writeString(file, "x".repeat(TokenFile.MAX_BYTES) + "\n" + "y".repeat(TokenFile.MIN_BYTES) + "\n");
        assertTrue(TokenFile.open(file).accepts("y".repeat(TokenFile.MIN_BYTES)));
    }
}
