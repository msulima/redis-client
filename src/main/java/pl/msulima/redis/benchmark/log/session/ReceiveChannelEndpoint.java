package pl.msulima.redis.benchmark.log.session;

import org.agrona.BufferUtil;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.transport.Transport;

import java.nio.ByteBuffer;

public class ReceiveChannelEndpoint {

    private static final int ALIGNMENT = 64;
    private final ByteBuffer buffer;
    private final OneToOneConcurrentArrayQueue<byte[]> responses;

    public ReceiveChannelEndpoint(int bufferSize, OneToOneConcurrentArrayQueue<byte[]> responses) {
        this.buffer = BufferUtil.allocateDirectAligned(bufferSize, ALIGNMENT).flip();
        this.responses = responses;
    }

    int receive(Transport transport) {
        if (responses.remainingCapacity() == 0) {
            return 0;
        }
        transport.receive(buffer);
        int remaining = buffer.remaining();
        if (remaining >= 0) {
            byte[] bytes = new byte[remaining];
            buffer.get(bytes);
            responses.add(bytes);
        }
        return remaining;
    }
}
