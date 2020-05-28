package pl.msulima.redis.benchmark.log.presentation;

import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.Request;
import pl.msulima.redis.benchmark.log.logbuffer.PublicationImage;
import pl.msulima.redis.benchmark.log.protocol.DynamicDecoder;
import pl.msulima.redis.benchmark.log.protocol.Response;
import pl.msulima.redis.benchmark.log.util.QueueUtil;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

class Receiver {

    private static final int CALLBACKS_BUFFER_SIZE = 1024;

    private final DynamicDecoder decoder = new DynamicDecoder();
    private final Queue<Request<?>> callbacksBuffer = new ArrayDeque<>(CALLBACKS_BUFFER_SIZE);
    private final OneToOneConcurrentArrayQueue<Request<?>> callbacks;
    private final PublicationImage image;
    private final OneToOneConcurrentArrayQueue<Request<?>> readyResponses;

    Receiver(OneToOneConcurrentArrayQueue<Request<?>> callbacks, PublicationImage image, OneToOneConcurrentArrayQueue<Request<?>> readyResponses) {
        this.callbacks = callbacks;
        this.image = image;
        this.readyResponses = readyResponses;
    }

    int doWork() {
        ByteBuffer buffer = image.readClaim();
        int position = buffer.position();
        while (true) {
            decoder.read(buffer);
            Response response = decoder.response;
            if (response.isComplete()) {
                onResponse(response);
            } else {
                break;
            }
        }
        image.commitRead(buffer);
        return buffer.position() - position;
    }

    private void onResponse(Response response) {
        Request<?> request = findRequest(response);
        request.complete(response);
        QueueUtil.offerOrSpin(readyResponses, request);
    }

    private Request<?> findRequest(Response response) {
        if (callbacksBuffer.isEmpty()) {
            callbacks.drainTo(callbacksBuffer, CALLBACKS_BUFFER_SIZE);
        }

        Request<?> request = callbacksBuffer.poll();
        if (request == null) {
            throw new IllegalStateException("Got response for unknown request " + response);
        }
        return request;
    }
}
