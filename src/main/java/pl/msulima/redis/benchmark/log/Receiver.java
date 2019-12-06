package pl.msulima.redis.benchmark.log;

import pl.msulima.redis.benchmark.log.protocol.Decoder;
import pl.msulima.redis.benchmark.log.protocol.Response;

import java.util.Queue;

class Receiver implements Runnable {

    private final Queue<Command> callbackQueue;

    Receiver(Queue<Command> callbackQueue) {
        this.callbackQueue = callbackQueue;
    }

    @Override
    public void run() {
        // TODO receive result
        byte[] result = null;
        Response response = Decoder.read(result);
        Command callback = callbackQueue.poll();
        //noinspection ConstantConditions
        callback.complete(response);
    }
}
