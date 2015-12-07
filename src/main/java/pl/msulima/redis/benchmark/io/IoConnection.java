package pl.msulima.redis.benchmark.io;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;


class IoConnection implements Closeable {

    private final Writer writer;
    private Socket socket;

    public IoConnection() {
        try {
            socket = new Socket("127.0.0.1", 6379);

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
