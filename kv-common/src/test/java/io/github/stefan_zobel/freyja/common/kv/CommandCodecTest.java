package io.github.stefan_zobel.freyja.common.kv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.common.DecodeException;
import io.github.stefan_zobel.freyja.common.kv.Command.Condition;
import io.github.stefan_zobel.freyja.common.kv.Command.Delete;
import io.github.stefan_zobel.freyja.common.kv.Command.Put;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.Failure;

/** The encoding of commands and results. */
class CommandCodecTest {

    private static byte[] b(String text) {
        return text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static List<Command> commands() {
        return List.of(
                new Command.RegisterSession(),
                new Command.KeepAlive(17),
                new Command.CloseSession(Long.MAX_VALUE),
                new Command.SetAlarm(Command.Alarm.NOSPACE),
                new Command.ClearAlarm(Command.Alarm.NOSPACE),
                new Command.SetClusterVersion(7),
                new Command.Write(5, 9, List.of()),
                new Command.Write(5, 10, List.of(new Put(b("k"), b("value")), new Delete(b("gone")),
                        new Put(b("empty"), new byte[0]))),
                new Command.Conditional(6, 1, List.of(Condition.exists(b("a")), Condition.notExists(b("b")),
                        Condition.valueEquals(b("c"), b("v")), Condition.modIndexEquals(b("d"), 99)),
                        List.of(new Put(b("x"), b("1"))), List.of(new Delete(b("x")))));
    }

    @Test
    void everyCommandHasOneEncodingAndSurvivesIt() {
        for (Command command : commands()) {
            byte[] bytes = CommandCodec.encode(123_456_789L, command);
            CommandCodec.Stamped decoded = CommandCodec.decode(bytes);
            assertEquals(123_456_789L, decoded.timestampMillis());
            assertEquals(command.getClass(), decoded.command().getClass());
            // Keys and values are arrays, which records compare by identity: compare the bytes.
            assertArrayEquals(bytes, CommandCodec.encode(123_456_789L, decoded.command()));
        }
    }

    @Test
    void everyResultSurvivesEncoding() {
        List<CommandResult> results = List.of(new CommandResult.Written(),
                new CommandResult.ConditionalApplied(true), new CommandResult.ConditionalApplied(false),
                new CommandResult.SessionRegistered(77), new CommandResult.SessionOk(),
                new CommandResult.AdminApplied(), new CommandResult.Failed(Failure.VERSION_TOO_OLD),
                new CommandResult.Failed(Failure.NO_SUCH_SESSION), new CommandResult.Failed(Failure.INVALID));
        for (CommandResult result : results) {
            assertEquals(result, CommandCodec.decodeResult(CommandCodec.encode(result)));
        }
        assertThrows(DecodeException.class, () -> CommandCodec.decodeResult(new byte[0]));
        assertThrows(DecodeException.class, () -> CommandCodec.decodeResult(new byte[] {9}));
        assertThrows(DecodeException.class, () -> CommandCodec.decodeResult(new byte[] {1, 0}));
        assertThrows(DecodeException.class, () -> CommandCodec.decodeResult(new byte[] {5, 99}));
    }

    @Test
    void whatIsNotOneCommandOfThisVersionIsRejected() {
        for (Command command : commands()) {
            byte[] bytes = CommandCodec.encode(1, command);
            for (int length = 0; length < bytes.length; length++) {
                byte[] cut = java.util.Arrays.copyOf(bytes, length);
                assertThrows(DecodeException.class, () -> CommandCodec.decode(cut));
            }
            byte[] longer = java.util.Arrays.copyOf(bytes, bytes.length + 1);
            assertThrows(DecodeException.class, () -> CommandCodec.decode(longer));
            byte[] otherVersion = bytes.clone();
            otherVersion[0] = 2;
            assertThrows(DecodeException.class, () -> CommandCodec.decode(otherVersion));
            byte[] otherType = bytes.clone();
            otherType[9] = 77;
            assertThrows(DecodeException.class, () -> CommandCodec.decode(otherType));
        }
        // A count that cannot fit must not make the decoder allocate for it.
        byte[] write = CommandCodec.encode(1, new Command.Write(1, 1, List.of()));
        write[write.length - 1] = 0x7F;
        assertThrows(DecodeException.class, () -> CommandCodec.decode(write));
    }
}
