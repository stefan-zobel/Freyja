package io.github.sz.freyja.node.concurrent;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.StructuredTaskScope;

/**
 * The one place of the project that uses structured concurrency, a preview feature of the
 * JDK. Everything else reaches it through the methods of this class, so that the project can
 * be built on a JDK without the feature by touching only this file.
 */
public final class Scopes {

    /** Handles one accepted connection. When it returns, the connection is closed. */
    @FunctionalInterface
    public interface ConnectionHandler {
        void handle(Socket socket) throws Exception;
    }

    /** A task of a connection. It may wait and may fail. */
    @FunctionalInterface
    public interface Task {
        void run() throws Exception;
    }

    private Scopes() {
    }

    /**
     * Runs two tasks, each in a virtual thread of its own, until one of them ends; then the
     * other is interrupted, and the call returns when both have ended. For the two halves of
     * a connection: when the reader is done, the writer has nothing left to wait for, and the
     * other way round.
     *
     * <p>A task that fails has ended like one that returns.
     */
    public static void either(Task first, Task second) throws InterruptedException {
        try (var scope = StructuredTaskScope.open(StructuredTaskScope.Joiner.<Boolean>anySuccessfulResultOrThrow())) {
            scope.fork(() -> ended(first));
            scope.fork(() -> ended(second));
            try {
                scope.join();
            } catch (StructuredTaskScope.FailedException e) {
                // cannot happen: a task that fails counts as ended
            }
        }
    }

    private static Boolean ended(Task task) {
        try {
            task.run();
        } catch (Exception e) {
            // the task has ended
        }
        return Boolean.TRUE;
    }

    /**
     * Accepts connections on {@code server} and handles each in a virtual thread of its own.
     * Returns when the server socket is closed, after every connection has been closed and
     * every handler has ended: no thread of a listener outlives the listener.
     *
     * <p>A failure of a handler ends its connection and nothing else.
     */
    public static void serve(ServerSocket server, ConnectionHandler handler) throws InterruptedException {
        Set<Socket> open = ConcurrentHashMap.newKeySet();
        try (var scope = StructuredTaskScope.open(StructuredTaskScope.Joiner.<Void>awaitAll())) {
            try {
                while (true) {
                    Socket socket;
                    try {
                        socket = server.accept();
                    } catch (SocketException e) {
                        break; // the listener was closed
                    } catch (IOException e) {
                        continue; // this one connection could not be accepted
                    }
                    open.add(socket);
                    scope.fork(() -> {
                        try (socket) {
                            handler.handle(socket);
                        } catch (Exception e) {
                            // the connection ends
                        } finally {
                            open.remove(socket);
                        }
                        return null;
                    });
                }
            } finally {
                // A handler waits in a read of its socket; closing the socket ends it.
                for (Socket socket : open) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        // closed already
                    }
                }
                scope.join();
            }
        }
    }
}
