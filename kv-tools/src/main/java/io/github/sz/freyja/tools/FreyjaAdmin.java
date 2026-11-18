package io.github.sz.freyja.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;

import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.Version;
import io.github.sz.freyja.common.admin.AdminCodec;
import io.github.sz.freyja.common.admin.AdminRequest;
import io.github.sz.freyja.common.admin.AdminResponse;
import io.github.sz.freyja.common.tls.Tls;
import io.github.sz.freyja.common.wire.Frames;
import io.github.sz.freyja.common.wire.Handshake;

/**
 * The command-line tool of an administrator (docs/spec/operations.md, section 1). It sends
 * one request to the administration listener of one node and prints the answer.
 *
 * <pre>
 * usage: FreyjaAdmin [TLS] HOST:PORT COMMAND [ARGUMENT]...
 *
 *   TLS, for a node that is not run in its insecure mode (section 5):
 *   --tls-key FILE             the private key and the certificate of the administrator (PKCS12)
 *   --tls-trust FILE           the authorities of the certificates of the nodes (PKCS12)
 *   --tls-password-file FILE   the password of both stores in its first line
 *   --tls-name ATTRIBUTE=VALUE what the subject of the certificate of the node must carry
 *
 *   status                             the state of the node
 *   add-learner N PEER-ADDR CLIENT-ADDR  announce node N and add it as a learner
 *   promote N [--force]                make learner N a voter
 *   remove N [--force]                 remove member N
 *   transfer N                         hand the leadership over to node N
 *   snapshot                           shorten the log of the node now
 *   scrub                              verify every block of the state machine of the node
 *   compact                            run the compactions that are due
 *   backup DIRECTORY                   write a backup into a new directory on the machine of the node
 *   clear-alarm                        clear the alarm for lack of space
 *   set-version V                      raise the cluster version (the last step of an upgrade)
 *   set-address N PEER-ADDR CLIENT-ADDR  tell the cluster where node N listens now
 * </pre>
 *
 * <p>Changes of the cluster go to the leader. Exit code 0: done. 1: refused, or the node
 * could not be reached. 2: wrong usage.
 */
public final class FreyjaAdmin {

    private FreyjaAdmin() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    public static int run(String[] args, PrintStream out, PrintStream err) {
        AdminRequest request;
        InetSocketAddress address;
        Tls tls;
        try {
            Path key = null;
            Path trust = null;
            Path password = null;
            String name = null;
            int options = 0;
            while (options + 1 < args.length && args[options].startsWith("--tls-")) {
                switch (args[options]) {
                    case "--tls-key" -> key = Path.of(args[options + 1]);
                    case "--tls-trust" -> trust = Path.of(args[options + 1]);
                    case "--tls-password-file" -> password = Path.of(args[options + 1]);
                    case "--tls-name" -> name = args[options + 1];
                    default -> throw new IllegalArgumentException("unknown option " + args[options]);
                }
                options += 2;
            }
            args = java.util.Arrays.copyOfRange(args, options, args.length);
            if (options > 0 && (key == null || trust == null)) {
                throw new IllegalArgumentException("TLS needs --tls-key and --tls-trust");
            }
            try {
                tls = options == 0 ? null : Tls.open(new Tls.Settings(key, trust, password, name));
            } catch (IOException e) {
                err.println("error: " + e.getMessage());
                return 1;
            }
            if (args.length < 2) {
                throw new IllegalArgumentException("a node and a command are needed");
            }
            int colon = args[0].lastIndexOf(':');
            if (colon <= 0) {
                throw new IllegalArgumentException("not HOST:PORT: " + args[0]);
            }
            address = new InetSocketAddress(args[0].substring(0, colon), Integer.parseInt(args[0].substring(colon + 1)));
            request = request(args);
        } catch (IllegalArgumentException e) {
            err.println("error: " + e.getMessage());
            err.println("usage: FreyjaAdmin [--tls-key FILE --tls-trust FILE [--tls-password-file FILE]"
                    + " [--tls-name ATTRIBUTE=VALUE]]");
            err.println("       HOST:PORT status | add-learner N PEER CLIENT | promote N [--force]"
                    + " | remove N [--force] | backup DIRECTORY"
                    + " | transfer N | snapshot | scrub | compact | clear-alarm | set-version V"
                    + " | set-address N PEER CLIENT");
            return 2;
        }
        try (Socket socket = new Socket()) {
            socket.connect(address, 5_000);
            socket.setSoTimeout(30_000);
            Socket secured = tls == null ? socket : tls.connect(socket, address.getHostString(), address.getPort());
            InputStream in = secured.getInputStream();
            OutputStream output = secured.getOutputStream();
            Frames.write(output, new Handshake(Handshake.ADMIN, 0, -1, Version.SOFTWARE).encode());
            output.flush();
            byte[] hello = Frames.read(in, Handshake.MAX_BYTES);
            if (hello == null) {
                throw new IOException("the node refused the connection; is this its administration listener?");
            }
            Handshake.decode(hello);
            Frames.write(output, AdminCodec.encode(1, request));
            output.flush();
            byte[] answer = Frames.read(in, 16 << 20);
            if (answer == null) {
                throw new IOException("the node closed the connection without an answer");
            }
            return print(AdminCodec.decodeResponse(answer).response(), out, err);
        } catch (IOException | DecodeException e) {
            err.println("error: " + e.getMessage());
            return 1;
        }
    }

    private static AdminRequest request(String[] args) {
        String command = args[1];
        boolean force = args[args.length - 1].equals("--force")
                && (command.equals("promote") || command.equals("remove"));
        int given = args.length - 2 - (force ? 1 : 0);
        int needed = switch (command) {
            case "status", "snapshot", "scrub", "compact", "clear-alarm" -> 0;
            case "promote", "remove", "transfer", "set-version", "backup" -> 1;
            case "add-learner", "set-address" -> 3;
            default -> throw new IllegalArgumentException("unknown command " + command);
        };
        if (given != needed) {
            throw new IllegalArgumentException(command + " takes " + needed + " arguments");
        }
        return switch (command) {
            case "status" -> new AdminRequest.Status();
            case "snapshot" -> new AdminRequest.Snapshot();
            case "scrub" -> new AdminRequest.Scrub();
            case "compact" -> new AdminRequest.Compact();
            case "clear-alarm" -> new AdminRequest.ClearSpaceAlarm();
            case "backup" -> new AdminRequest.Backup(args[2]);
            case "promote" -> new AdminRequest.PromoteLearner(Integer.parseInt(args[2]), force);
            case "remove" -> new AdminRequest.RemoveMember(Integer.parseInt(args[2]), force);
            case "transfer" -> new AdminRequest.TransferLeadership(Integer.parseInt(args[2]));
            case "set-version" -> new AdminRequest.SetClusterVersion(Integer.parseInt(args[2]));
            case "add-learner" -> new AdminRequest.AddLearner(Integer.parseInt(args[2]), args[3], args[4]);
            default -> new AdminRequest.SetAddress(Integer.parseInt(args[2]), args[3], args[4]);
        };
    }

    private static int print(AdminResponse response, PrintStream out, PrintStream err) {
        switch (response) {
            case AdminResponse.Done done -> {
                out.println(done.detail());
                return 0;
            }
            case AdminResponse.Refused refused -> {
                err.println("refused: " + refused.reason()
                        + (refused.leaderId() >= 0 ? " (the leader is node " + refused.leaderId() + ")" : ""));
                return 1;
            }
            case AdminResponse.Status status -> {
                out.println("node:            " + status.nodeId() + " of cluster " + Long.toHexString(status.clusterId())
                        + ", software " + status.software());
                out.println("role:            " + status.role() + " in term " + status.term()
                        + (status.leaderId() >= 0 ? ", leader is node " + status.leaderId() : ", no leader known"));
                out.println("log:             last " + status.lastLogIndex() + ", committed " + status.commitIndex()
                        + ", applied " + status.appliedIndex() + ", starts after " + status.snapshotIndex()
                        + (status.logRecovering() ? ", LOG-RECOVERING" : ""));
                out.println("cluster version: " + status.clusterVersion());
                out.println("alarms:          " + (status.spaceAlarm() ? "NOSPACE" : "none"));
                out.println("restarts:        " + status.restarts() + ", snapshots installed "
                        + status.snapshotsInstalled());
                for (AdminResponse.Member member : status.members()) {
                    out.println("member:          node " + member.nodeId() + " " + (member.voter() ? "voter" : "learner")
                            + (member.peerAddress().isEmpty() ? "" : ", peers at " + member.peerAddress())
                            + (member.clientAddress().isEmpty() ? "" : ", clients at " + member.clientAddress())
                            + (member.matchIndex() < 0 ? "" : ", log agrees through " + member.matchIndex())
                            + (member.state().isEmpty() ? "" : ", " + member.state()));
                }
                return 0;
            }
        }
    }
}
