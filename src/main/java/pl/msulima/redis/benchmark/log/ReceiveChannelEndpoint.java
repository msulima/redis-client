package pl.msulima.redis.benchmark.log;

import pl.msulima.redis.benchmark.log.protocol.Response;

import java.util.Queue;

public class ReceiveChannelEndpoint {

    private final Queue<Command<?>> callbackQueue;

    public ReceiveChannelEndpoint(Queue<Command<?>> callbackQueue) {
        this.callbackQueue = callbackQueue;
    }

    public void onResponse(Response response) {
        Command command = callbackQueue.poll();
        if (command == null) {
            throw new IllegalStateException("Got response for unknown request " + response);
        }
        command.complete(response);
    }
}
