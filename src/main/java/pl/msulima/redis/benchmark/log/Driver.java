package pl.msulima.redis.benchmark.log;

import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.network.RedisTransportPoller;
import pl.msulima.redis.benchmark.log.protocol.Response;
import redis.clients.jedis.Protocol;

import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

class Driver {

    private final Queue<Command> requestQueue;
    private final Sender sender;
    private final Receiver receiver;

    Driver(int capacity) {
        this.requestQueue = new ManyToOneConcurrentArrayQueue<>(capacity);
        Queue<Command> callbacksQueue = new OneToOneConcurrentArrayQueue<>(capacity);
        this.sender = new Sender(requestQueue, callbacksQueue);
        this.receiver = new Receiver(new RedisTransportPoller(1024));
    }

    <T> CompletionStage<T> offer(Protocol.Command cmd, Function<Response, T> deserializer, byte[]... args) {
        Command<T> command = new Command<>(cmd, deserializer, args);
        // TODO handle queue full
        requestQueue.offer(command);
        return command.getPromise();
    }

    void run() {
        sender.run();
        receiver.doWork();
    }
}
