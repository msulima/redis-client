package pl.msulima.redis.benchmark.io;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;


class IoConnection implements Closeable {

    private final Writer writer;
    private Socket socket;

    public IoConnection(String host, int port) {
        int timeout = 3000;
        try {
            socket = new Socket();

            socket.setReuseAddress(true);
            socket.setKeepAlive(true);
            socket.setTcpNoDelay(true);
            socket.setSoLinger(true, 0);

            socket.connect(new InetSocketAddress(host, port), timeout);

            Reader reader = new Reader(socket.getInputStream());
            writer = new Writer(socket.getOutputStream(), reader);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void submit(Command command) {
        writer.write(command);
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
