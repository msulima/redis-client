package pl.msulima.redis.benchmark.log.session;

import org.agrona.BufferUtil;
import pl.msulima.redis.benchmark.log.Request;
import pl.msulima.redis.benchmark.log.protocol.DynamicEncoder;
import pl.msulima.redis.benchmark.log.transport.Transport;

import java.nio.ByteBuffer;
import java.util.Queue;

public class SendChannelEndpoint {

    private final Queue<Request<?>> requestsQueue;
    private final Queue<Request<?>> callbacksQueue;
    private final ByteBuffer buffer;
    private final DynamicEncoder encoder = new DynamicEncoder();

    public SendChannelEndpoint(Queue<Request<?>> requestsQueue, Queue<Request<?>> callbacksQueue, int bufferSize) {
        this.requestsQueue = requestsQueue;
        this.callbacksQueue = callbacksQueue;
        this.buffer = BufferUtil.allocateDirectAligned(bufferSize, 64).flip();
    }

    int send(Transport transport) {
        transport.send(buffer);
        if (buffer.hasRemaining()) {
            return buffer.remaining();
        }

        buffer.clear();
        while (encoder.write(buffer)) {
            Request<?> request = poll();
            if (request != null) {
                encoder.setRequest(request.command, request.args);
            } else {
                break;
            }
        }
        buffer.flip();
        return buffer.remaining();
    }

    private Request<?> poll() {
        Request<?> request = requestsQueue.peek();
        if (request != null) {
            if (callbacksQueue.offer(request)) {
                requestsQueue.remove();
                // TODO  what if send fails?
                return request;
            }
        }
        return null;
    }
}
