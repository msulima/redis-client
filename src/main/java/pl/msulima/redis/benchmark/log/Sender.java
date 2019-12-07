package pl.msulima.redis.benchmark.log;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import pl.msulima.redis.benchmark.log.protocol.Encoder;

import java.util.Queue;

class Sender implements Runnable {

    private final Queue<Command<?>> requestQueue;
    private final Queue<Command<?>> callbacksQueue;
    private IdleStrategy idleStrategy = new BackoffIdleStrategy(100, 100, 1000, 10000);

    Sender(Queue<Command<?>> requestQueue, Queue<Command<?>> callbacksQueue) {
        this.requestQueue = requestQueue;
        this.callbacksQueue = callbacksQueue;
    }

    @Override
    public void run() {
        Command command;
        while ((command = requestQueue.poll()) == null) {
            idleStrategy.idle();
        }
        callbacksQueue.offer(command);
        byte[] payload = Encoder.write(command.command, command.args);
        // TODO send
    }
}
