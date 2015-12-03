package pl.msulima.redis.benchmark.nio;

import pl.msulima.redis.benchmark.jedis.Operation;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.util.Optional;

public class Reader {

    private final OneToOneConcurrentArrayQueue<Operation> pending;
    private final Parser parser = new Parser();
    private final ByteBuffer buffer = ByteBuffer.allocate(1024);

    public Reader(OneToOneConcurrentArrayQueue<Operation> pending) {
        this.pending = pending;
    }

    void read(SelectionKey key) throws IOException {
        ReadableByteChannel channel = (ReadableByteChannel) key.channel();

        int read = channel.read(buffer);

        if (read > 0) {
            buffer.flip();
            System.out.println("<-- " + buffer.limit() + " " + new String(buffer.array(), 0, buffer.limit()));
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

    private void debug(Object r) {
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