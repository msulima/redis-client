package pl.msulima.redis.benchmark.log;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import pl.msulima.redis.benchmark.log.network.Transport;
import pl.msulima.redis.benchmark.log.protocol.Encoder;

import java.util.Queue;

class Sender implements Agent {

    private final Queue<Command<?>> requestQueue;
    private final Queue<Command<?>> callbacksQueue;
    private IdleStrategy idleStrategy = new BackoffIdleStrategy(100, 100, 1000, 10000);

    Sender(Queue<Command<?>> requestQueue, Queue<Command<?>> callbacksQueue) {
        this.requestQueue = requestQueue;
        this.callbacksQueue = callbacksQueue;
    }

    public void registerChannelEndpoint(SendChannelEndpoint sendChannelEndpoint, Transport transport) {

    }

    @Override
    public String roleName() {
        return "sender";
    }

    @Override
    public int doWork() {
        Command command;
        while ((command = requestQueue.poll()) == null) {
            idleStrategy.idle();
        }
        callbacksQueue.offer(command);
        byte[] payload = Encoder.write(command.command, command.args);
        // TODO send
        return 0;
    }

    @Override
    public void onClose() {

    }
}
