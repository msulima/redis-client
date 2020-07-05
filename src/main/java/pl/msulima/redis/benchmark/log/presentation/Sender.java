package pl.msulima.redis.benchmark.log.presentation;

import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.Request;
import pl.msulima.redis.benchmark.log.logbuffer.PublicationImage;
import pl.msulima.redis.benchmark.log.protocol.DynamicEncoder;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

class Sender {

    private static final int REQUESTS_BUFFER_SIZE = 1024;

    private final DynamicEncoder encoder = new DynamicEncoder();
    private final Queue<Request<?>> requestsBuffer = new ArrayDeque<>(REQUESTS_BUFFER_SIZE);
    private final ManyToOneConcurrentArrayQueue<Request<?>> requests;
    private final OneToOneConcurrentArrayQueue<Request<?>> callbacks;
    private final PublicationImage image;

    Sender(ManyToOneConcurrentArrayQueue<Request<?>> requests, OneToOneConcurrentArrayQueue<Request<?>> callbacks, PublicationImage image) {
        this.requests = requests;
        this.callbacks = callbacks;
        this.image = image;
    }

    int doWork() {
        ByteBuffer buffer = image.writeClaim();
        int startPosition = buffer.position();
        while (encoder.write(buffer)) {
            Request<?> request = poll();
            if (request != null) {
                encoder.setRequest(request.command, request.args);
            } else {
                break;
            }
        }
        image.commitWrite(buffer);
        return buffer.position() - startPosition;
    }

    private Request<?> poll() {
        if (requestsBuffer.isEmpty()) {
            requests.drainTo(requestsBuffer, REQUESTS_BUFFER_SIZE);
        }

        Request<?> request = requestsBuffer.peek();
        if (request != null) {
            if (callbacks.offer(request)) {
                requestsBuffer.remove();
                // TODO  what if send fails?
                return request;
            }
        }
        return null;
    }
}
