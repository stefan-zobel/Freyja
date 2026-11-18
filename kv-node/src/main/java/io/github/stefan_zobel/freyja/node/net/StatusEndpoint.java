package io.github.stefan_zobel.freyja.node.net;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * The HTTP endpoint for metrics and health (docs/spec/operations.md, section 2), on the HTTP
 * server that comes with the JDK.
 *
 * <ul>
 *   <li>{@code GET /metrics}: the metrics as plain text.</li>
 *   <li>{@code GET /health}: 200 if the process runs its node; 503 with the reason if the
 *       node has ended for good.</li>
 *   <li>{@code GET /ready}: 200 if the node can serve; 503 with the reason if not.</li>
 * </ul>
 */
public final class StatusEndpoint implements Closeable {

    private final HttpServer server;
    private final ExecutorService executor;

    /**
     * @param metrics   gives the text for {@code /metrics}
     * @param unhealthy gives the reason why the node is not alive, or {@code null} if it is
     * @param notReady  gives the reason why the node cannot serve, or {@code null} if it can
     */
    public StatusEndpoint(InetSocketAddress bind, Supplier<String> metrics, Supplier<String> unhealthy,
                          Supplier<String> notReady) throws IOException {
        server = HttpServer.create(bind, 16);
        executor = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(executor);
        server.createContext("/metrics", exchange -> answer(exchange, 200, metrics.get()));
        server.createContext("/health", exchange -> verdict(exchange, unhealthy.get()));
        server.createContext("/ready", exchange -> verdict(exchange, notReady.get()));
        server.start();
    }

    private static void verdict(HttpExchange exchange, String reason) throws IOException {
        answer(exchange, reason == null ? 200 : 503, reason == null ? "ok\n" : reason + "\n");
    }

    private static void answer(HttpExchange exchange, int status, String text) throws IOException {
        try (exchange) {
            if (!exchange.getRequestMethod().equals("GET")) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            byte[] body = text.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
    }

    /** The port the endpoint listens on. */
    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }
}
