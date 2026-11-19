package io.github.sz.freyja.bench;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.Version;
import io.github.sz.freyja.common.admin.AdminCodec;
import io.github.sz.freyja.common.admin.AdminRequest;
import io.github.sz.freyja.common.admin.AdminResponse;
import io.github.sz.freyja.common.wire.Frames;
import io.github.sz.freyja.common.wire.Handshake;

/**
 * What a benchmark asks of the nodes besides requests of clients: their figures, from the
 * HTTP endpoint for metrics, and changes through the administration listener
 * (docs/spec/operations.md, sections 1 and 2). Nodes in insecure mode only, like the clients
 * of the load generator.
 */
final class NodeAccess {

    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private NodeAccess() {
    }

    /**
     * The figures of a node as its endpoint shows them: every line {@code name value} of
     * {@code /metrics}.
     *
     * @param endpoint the address of the HTTP endpoint of the node, as {@code host:port}
     */
    static Map<String, Long> figures(String endpoint) throws IOException {
        HttpResponse<String> response;
        try {
            response = HTTP.send(HttpRequest.newBuilder(URI.create("http://" + endpoint + "/metrics"))
                    .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while asking " + endpoint + " for its figures", e);
        } catch (IllegalArgumentException e) {
            throw new IOException("not host:port: " + endpoint, e);
        }
        if (response.statusCode() != 200) {
            throw new IOException(endpoint + " answered " + response.statusCode() + " to /metrics");
        }
        Map<String, Long> figures = new HashMap<>();
        for (String line : response.body().split("\n")) {
            int space = line.indexOf(' ');
            if (space > 0) {
                try {
                    figures.put(line.substring(0, space), Long.parseLong(line.substring(space + 1).trim()));
                } catch (NumberFormatException e) {
                    // not a figure
                }
            }
        }
        return figures;
    }

    /**
     * Sends one request to the administration listener of a node and returns the answer.
     *
     * @param endpoint the address of the administration listener, as {@code host:port}
     */
    static AdminResponse admin(String endpoint, AdminRequest request) throws IOException {
        int colon = endpoint.lastIndexOf(':');
        if (colon <= 0) {
            throw new IOException("not host:port: " + endpoint);
        }
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(endpoint.substring(0, colon),
                    Integer.parseInt(endpoint.substring(colon + 1))), 5_000);
            socket.setSoTimeout(60_000);
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
            Frames.write(out, new Handshake(Handshake.ADMIN, 0, -1, Version.SOFTWARE).encode());
            out.flush();
            byte[] hello = Frames.read(in, Handshake.MAX_BYTES);
            if (hello == null) {
                throw new IOException(endpoint + " refused the connection; is it an administration listener?");
            }
            Handshake.decode(hello);
            Frames.write(out, AdminCodec.encode(1, request));
            out.flush();
            byte[] answer = Frames.read(in, 16 << 20);
            if (answer == null) {
                throw new IOException(endpoint + " closed the connection without an answer");
            }
            return AdminCodec.decodeResponse(answer).response();
        } catch (DecodeException | NumberFormatException e) {
            throw new IOException(endpoint + ": " + e.getMessage(), e);
        }
    }

    /** The state of a node; fails if the node answers with anything else. */
    static AdminResponse.Status status(String endpoint) throws IOException {
        AdminResponse response = admin(endpoint, new AdminRequest.Status());
        if (!(response instanceof AdminResponse.Status status)) {
            throw new IOException(endpoint + " answered " + response + " to a request for its state");
        }
        return status;
    }
}
