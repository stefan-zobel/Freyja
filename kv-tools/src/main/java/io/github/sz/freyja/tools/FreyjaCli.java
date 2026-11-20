package io.github.sz.freyja.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

import io.github.sz.freyja.client.FreyjaClient;
import io.github.sz.freyja.client.FreyjaException;
import io.github.sz.freyja.common.tls.Tls;

/**
 * The command-line tool of a user: reads and writes keys through the client library
 * (docs/spec/node.md, section 9). Every call opens a client, does one thing, and closes it.
 *
 * <pre>
 * usage: FreyjaCli [OPTION]... HOST:PORT[,HOST:PORT]... COMMAND [ARGUMENT]...
 *
 *   --tls-trust FILE           the authorities of the certificates of the nodes (PKCS12); TLS
 *   --tls-key FILE             the private key and the certificate of the user (PKCS12)
 *   --tls-password-file FILE   the password of both stores in its first line
 *   --tls-name ATTRIBUTE=VALUE what the subject of the certificate of a node must carry
 *   --token-file FILE          the token of a user without a certificate, in its first line
 *   --timeout-ms N             how long the command may take (default 10,000)
 *   --hex                      keys and values are given and printed in hexadecimal
 *
 *   put KEY VALUE                      set the value of a key
 *   get KEY [--stale]                  print the value of a key
 *   delete KEY                         remove a key
 *   scan [START [END]] [--limit N] [--stale]
 *                                      print the keys from START (inclusive) to END
 *                                      (exclusive) with their values, one pair per line,
 *                                      at most N (default 100)
 * </pre>
 *
 * <p>Keys and values are UTF-8 text unless {@code --hex} is given. A read is linearizable
 * unless {@code --stale} is given. Exit code 0: done. 1: the cluster could not be reached,
 * refused, or did not answer in time; a write whose outcome is not known says so. 2: wrong
 * usage. 3: {@code get} found no value.
 */
public final class FreyjaCli {

    private static final String USAGE = """
            usage: FreyjaCli [--tls-trust FILE [--tls-key FILE] [--tls-password-file FILE]
                             [--tls-name ATTRIBUTE=VALUE] [--token-file FILE]] [--timeout-ms N] [--hex]
                             HOST:PORT[,HOST:PORT]... put KEY VALUE | get KEY [--stale] | delete KEY
                             | scan [START [END]] [--limit N] [--stale]""";

    private FreyjaCli() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    /** A command, read from its arguments. */
    private record Command(String name, List<byte[]> arguments, boolean stale, int limit) {
    }

    public static int run(String[] args, PrintStream out, PrintStream err) {
        FreyjaClient.Options options;
        Command command;
        boolean hex = false;
        try {
            Path key = null;
            Path trust = null;
            Path password = null;
            Path tokenFile = null;
            String name = null;
            int timeout = 10_000;
            int at = 0;
            while (at < args.length && args[at].startsWith("--")) {
                String option = args[at];
                if (option.equals("--hex")) {
                    hex = true;
                    at++;
                    continue;
                }
                if (at + 1 >= args.length) {
                    throw new IllegalArgumentException(option + " needs a value");
                }
                String value = args[at + 1];
                switch (option) {
                    case "--tls-key" -> key = Path.of(value);
                    case "--tls-trust" -> trust = Path.of(value);
                    case "--tls-password-file" -> password = Path.of(value);
                    case "--tls-name" -> name = value;
                    case "--token-file" -> tokenFile = Path.of(value);
                    case "--timeout-ms" -> timeout = positive(option, value);
                    default -> throw new IllegalArgumentException("unknown option " + option);
                }
                at += 2;
            }
            if (args.length - at < 2) {
                throw new IllegalArgumentException("nodes and a command are needed");
            }
            if (trust == null && (key != null || password != null || name != null || tokenFile != null)) {
                throw new IllegalArgumentException("TLS needs --tls-trust");
            }
            options = FreyjaClient.Options.of(args[at].split(",")).withRequestTimeout(timeout).withKeepAlive(0);
            if (trust != null) {
                String token = null;
                if (tokenFile != null) {
                    try {
                        token = Files.readAllLines(tokenFile, StandardCharsets.UTF_8).stream().findFirst().orElse("")
                                .strip();
                    } catch (IOException e) {
                        err.println("error: the token file cannot be read: " + e.getMessage());
                        return 1;
                    }
                }
                options = options.withTls(new Tls.Settings(key, trust, password, name), token);
            }
            command = command(Arrays.copyOfRange(args, at + 1, args.length), hex);
        } catch (IllegalArgumentException e) {
            err.println("error: " + e.getMessage());
            err.println(USAGE);
            return 2;
        }
        try (FreyjaClient client = FreyjaClient.open(options)) {
            return execute(client, command, hex, out, err);
        } catch (FreyjaException.UnknownOutcome e) {
            err.println("error: the outcome is not known; the write may or may not have taken effect: "
                    + e.getMessage());
            return 1;
        } catch (FreyjaException | UncheckedIOException e) {
            err.println("error: " + e.getMessage());
            return 1;
        }
    }

    private static int execute(FreyjaClient client, Command command, boolean hex, PrintStream out,
                               PrintStream err) {
        List<byte[]> a = command.arguments();
        switch (command.name()) {
            case "put" -> {
                client.put(a.get(0), a.get(1));
                out.println("ok");
            }
            case "delete" -> {
                client.delete(a.get(0));
                out.println("ok");
            }
            case "get" -> {
                Optional<FreyjaClient.Versioned> value = command.stale() ? client.getStale(a.get(0))
                        : client.get(a.get(0));
                if (value.isEmpty()) {
                    err.println("no value");
                    return 3;
                }
                out.println(text(value.get().value(), hex));
            }
            case "scan" -> {
                byte[] start = a.isEmpty() ? new byte[0] : a.get(0);
                byte[] end = a.size() < 2 ? new byte[0] : a.get(1);
                List<FreyjaClient.Entry> entries = command.stale() ? client.scanStale(start, end, command.limit())
                        : client.scan(start, end, command.limit());
                for (FreyjaClient.Entry entry : entries) {
                    out.println(text(entry.key(), hex) + "\t" + text(entry.value(), hex));
                }
            }
            default -> throw new IllegalStateException(command.name());
        }
        return 0;
    }

    private static Command command(String[] args, boolean hex) {
        boolean stale = false;
        int limit = 100;
        List<byte[]> arguments = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            if (args[i].equals("--stale")) {
                stale = true;
            } else if (args[i].equals("--limit")) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("--limit needs a value");
                }
                limit = positive("--limit", args[++i]);
            } else {
                arguments.add(bytes(args[i], hex));
            }
        }
        String name = args[0];
        boolean fits = switch (name) {
            case "put" -> arguments.size() == 2 && !stale && limit == 100;
            case "get" -> arguments.size() == 1 && limit == 100;
            case "delete" -> arguments.size() == 1 && !stale && limit == 100;
            case "scan" -> arguments.size() <= 2;
            default -> throw new IllegalArgumentException("unknown command " + name);
        };
        if (!fits) {
            throw new IllegalArgumentException("wrong arguments for " + name);
        }
        return new Command(name, arguments, stale, limit);
    }

    private static int positive(String option, String value) {
        try {
            int n = Integer.parseInt(value);
            if (n > 0) {
                return n;
            }
        } catch (NumberFormatException e) {
            // said below
        }
        throw new IllegalArgumentException(option + " needs a positive number, not " + value);
    }

    private static byte[] bytes(String argument, boolean hex) {
        if (!hex) {
            return argument.getBytes(StandardCharsets.UTF_8);
        }
        try {
            return HexFormat.of().parseHex(argument);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("not hexadecimal: " + argument);
        }
    }

    private static String text(byte[] bytes, boolean hex) {
        return hex ? HexFormat.of().formatHex(bytes) : new String(bytes, StandardCharsets.UTF_8);
    }
}
