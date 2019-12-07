package pl.msulima.redis.benchmark.log;

import pl.msulima.redis.benchmark.log.protocol.Response;

import java.util.Queue;

public class ReceiveChannelEndpoint {

    private final Queue<Command<?>> callbackQueue;

    public ReceiveChannelEndpoint(Queue<Command<?>> callbackQueue) {
        this.callbackQueue = callbackQueue;
    }

    public void onResponse(Response response) {
        Command poll = callbackQueue.poll();
        if (poll == null) {
            throw new IllegalStateException("Got response for unknown request " + response);
        }
        poll.complete(response);
    }
}
