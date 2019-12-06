package pl.msulima.redis.benchmark.log.network;

import java.io.*;

public class InMemoryConnection {

    private final InputStream inputStream;
    private final OutputStream outputStream;

    public InMemoryConnection() {
        PipedInputStream pipedInputStream = new PipedInputStream(1024);
        this.inputStream = pipedInputStream;
        try {
            this.outputStream = new PipedOutputStream(pipedInputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void write(byte[] payload) {
        try {
            outputStream.write(payload);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void read() {

    }
}
