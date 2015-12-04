package pl.msulima.redis.benchmark.nio;

import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.util.Optional;

public class Reader {

    private final OneToOneConcurrentArrayQueue<Operation> pending;
    private final Parser parser = new Parser();
    private final ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
    private final boolean debugEnabled = Boolean.parseBoolean(System.getProperty("debugEnabled", "false"));
    private final int maxBytesRead = Integer.parseInt(System.getProperty("maxBytesRead", "0"));

    public Reader(OneToOneConcurrentArrayQueue<Operation> pending) {
        this.pending = pending;
    }

    void read(SelectionKey key) throws IOException {
        ReadableByteChannel channel = (ReadableByteChannel) key.channel();

        int read = readFrom(channel);

        if (read > 0) {
            buffer.flip();
        }

        Optional<?> response;
        while (true) {
            buffer.mark();
            response = parser.parse(buffer);

            if (response.isPresent()) {
                this.pending.poll().done(response.get());
                debug(response.get());
            } else {
                buffer.reset();
                break;
            }
        }

        if (buffer.position() < buffer.limit()) {
            buffer.compact();
        } else {
            buffer.clear();
        }

        if (pending.isEmpty()) {
            key.interestOps(SelectionKey.OP_WRITE);
        }
    }

    private int readFrom(ReadableByteChannel channel) throws IOException {
        int bytesRead;

        if (maxBytesRead > 0) {
            int limit = buffer.limit();
            int position = buffer.position();
            buffer.limit(Math.min(position + maxBytesRead, buffer.limit()));

            bytesRead = channel.read(buffer);

            buffer.limit(limit);
        } else {
            bytesRead = channel.read(buffer);
        }

        return bytesRead;
    }

    private void debug(Object r) {
        if (debugEnabled) {
            if (r instanceof String) {
                System.out.println("<- SS " + r);
            } else if (r instanceof Integer) {
                System.out.println("<- IN " + r);
            } else {
                Optional<byte[]> r1 = (Optional<byte[]>) r;
                System.out.println("<- BS " + r1.map(String::new).orElse("<null>"));
            }
        }
    }
}