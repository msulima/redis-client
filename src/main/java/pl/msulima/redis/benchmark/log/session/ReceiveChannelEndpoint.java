package pl.msulima.redis.benchmark.log.session;

import org.agrona.BufferUtil;
import pl.msulima.redis.benchmark.log.Request;
import pl.msulima.redis.benchmark.log.protocol.DynamicDecoder;
import pl.msulima.redis.benchmark.log.protocol.Response;
import pl.msulima.redis.benchmark.log.transport.Transport;

import java.nio.ByteBuffer;
import java.util.Queue;

public class ReceiveChannelEndpoint {

    private final Queue<Request<?>> callbackQueue;
    private final ByteBuffer buffer;
    private final DynamicDecoder decoder = new DynamicDecoder();

    public ReceiveChannelEndpoint(Queue<Request<?>> callbackQueue, int bufferSize) {
        this.callbackQueue = callbackQueue;
        this.buffer = BufferUtil.allocateDirectAligned(bufferSize, 64).flip();
    }

    int receive(Transport transport) {
        transport.receive(buffer);
        while (true) {
            decoder.read(buffer);
            Response response = decoder.response;
            if (response.isComplete()) {
                onResponse(response);
            } else {
                break;
            }
        }
        return buffer.position();
    }

    private void onResponse(Response response) {
        Request<?> request = callbackQueue.poll();
        if (request == null) {
            throw new IllegalStateException("Got response for unknown request " + response);
        }
        request.complete(response);
    }
}
