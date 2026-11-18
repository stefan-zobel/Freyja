package io.github.sz.freyja.tools.external;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A TCP proxy for one direction of one link between two nodes: the connection over which one
 * node sends to another goes through it (docs/spec/verification.md, section 4).
 *
 * <p>The test tells it what the network does:
 * <ul>
 *   <li>{@link #block}: nothing passes. Connections stay open and new ones are accepted, as
 *       with a network that swallows packets: the sender learns nothing.</li>
 *   <li>{@link #delay}: everything waits that long on its way.</li>
 *   <li>{@link #reset}: every connection breaks; what was on its way is lost.</li>
 *   <li>{@link #heal}: the network is well again. Connections that were blocked are broken,
 *       so that nothing of what they held arrives late and out of its time.</li>
 * </ul>
 */
final class FaultProxy implements Closeable {

    private final ServerSocket listener;
    private final InetSocketAddress target;
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private volatile boolean blocked;
    private volatile int delayMillis;
    private volatile boolean closed;

    /** Listens on a free port of the loopback interface and forwards to {@code targetPort}. */
    FaultProxy(int targetPort) throws IOException {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        this.listener = new ServerSocket(0, 50, loopback);
        this.target = new InetSocketAddress(loopback, targetPort);
        Thread.ofVirtual().name("proxy-accept-" + targetPort).start(this::accept);
    }

    int port() {
        return listener.getLocalPort();
    }

    void block() {
        blocked = true;
    }

    void delay(int millis) {
        delayMillis = millis;
    }

    /** Breaks every connection. The nodes open new ones. */
    void reset() {
        for (Socket socket : sockets) {
            closeQuietly(socket);
        }
    }

    void heal() {
        boolean wasBlocked = blocked;
        blocked = false;
        delayMillis = 0;
        if (wasBlocked) {
            reset();
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        listener.close();
        reset();
    }

    private void accept() {
        while (!closed) {
            Socket from;
            try {
                from = listener.accept();
            } catch (IOException e) {
                return; // closed
            }
            sockets.add(from);
            Thread.ofVirtual().name("proxy-connect").start(() -> connect(from));
        }
    }

    private void connect(Socket from) {
        Socket to = new Socket();
        sockets.add(to);
        try {
            // A blocked link does not even reach the other side.
            while (blocked && !closed && !from.isClosed()) {
                Thread.sleep(20);
            }
            if (closed || from.isClosed()) {
                throw new IOException("gone while blocked");
            }
            to.connect(target, 2_000);
            from.setTcpNoDelay(true);
            to.setTcpNoDelay(true);
            Thread.ofVirtual().name("proxy-back").start(() -> pump(to, from));
            pump(from, to);
        } catch (IOException e) {
            closeQuietly(from);
            closeQuietly(to);
        } catch (InterruptedException e) {
            closeQuietly(from);
            closeQuietly(to);
            Thread.currentThread().interrupt();
        } finally {
            sockets.remove(from);
            sockets.remove(to);
        }
    }

    /** Copies one direction of a connection until it ends; then both sockets are closed. */
    private void pump(Socket source, Socket sink) {
        byte[] buffer = new byte[16 * 1024];
        try {
            InputStream in = source.getInputStream();
            OutputStream out = sink.getOutputStream();
            int n;
            while ((n = in.read(buffer)) >= 0) {
                int wait = delayMillis;
                if (wait > 0) {
                    Thread.sleep(wait);
                }
                while (blocked && !closed) {
                    Thread.sleep(20); // held back; a heal breaks the connection
                }
                out.write(buffer, 0, n);
                out.flush();
            }
        } catch (IOException e) {
            // the connection is over
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            closeQuietly(source);
            closeQuietly(sink);
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            // closing is all that was wanted
        }
    }
}
