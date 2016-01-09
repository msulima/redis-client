package pl.msulima.redis.benchmark.io;

import redis.clients.jedis.Protocol;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.function.BiConsumer;


class IoConnection implements Closeable {

    private final Writer writer;
    private Socket socket;
    public static final int BUFFER_SIZE = 1024 * 1024;

    public IoConnection(String host, int port) {
        int timeout = 3000;
        try {
            socket = new Socket();

            socket.setReuseAddress(true);
            socket.setKeepAlive(true);
            socket.setTcpNoDelay(true);
            socket.setSoLinger(true, 0);
            socket.setSendBufferSize(BUFFER_SIZE);
            socket.setReceiveBufferSize(BUFFER_SIZE);

            socket.connect(new InetSocketAddress(host, port), timeout);

            Reader reader = new Reader(socket.getInputStream(), BUFFER_SIZE);
            writer = new Writer(socket.getOutputStream(), reader, BUFFER_SIZE);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public <T> void submit(Protocol.Command command, BiConsumer<T, Throwable> callback, byte[][] arguments) {
        writer.write(command, callback, arguments);
    }

    @Override
    public void close() {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
