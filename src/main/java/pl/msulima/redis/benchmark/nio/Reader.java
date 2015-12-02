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
        channel.read(buffer);
        buffer.flip();

        System.out.println("<-- " + new String(buffer.array(), 0, buffer.limit()));

        Optional<Object> response;
        while ((response = parser.parse(buffer)).isPresent()) {
            response.ifPresent(r -> {
                this.pending.poll().done(r);
                debug(r);
            });
        }

        buffer.flip();

        if (pending.isEmpty()) {
            key.interestOps(SelectionKey.OP_WRITE);
        } else {
            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        }
    }

    private void debug(Object r) {
        if (r instanceof String) {
            System.out.println("<- SS " + r);
        } else {
            System.out.println("<- BS " + new String((byte[]) r));
        }
    }
}