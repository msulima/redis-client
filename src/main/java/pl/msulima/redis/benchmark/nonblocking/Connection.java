package pl.msulima.redis.benchmark.nonblocking;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public class Connection {

    private final Writer writer;
    private final Reader reader;

    public Connection(Writer writer, Reader reader) {
        this.writer = writer;
        this.reader = reader;
    }

    public void read(SocketChannel channel) throws IOException {
        reader.read(channel);
    }

    public void write(SocketChannel channel) throws IOException {
        writer.write(channel);
    }
}
