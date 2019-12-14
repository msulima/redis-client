package pl.msulima.redis.benchmark.log;

import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.protocol.Command;
import pl.msulima.redis.benchmark.log.protocol.Response;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class Connection {

    private static final int MAX_RETRIES = 100;

    private final ManyToOneConcurrentArrayQueue<Request<?>> requestQueue;
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy(0, 5L, 1000L, 1_000_000L);

    public Connection(ManyToOneConcurrentArrayQueue<Request<?>> requestQueue) {
        this.requestQueue = requestQueue;
    }

    <T> CompletionStage<T> offer(Command cmd, Function<Response, T> deserializer, byte[]... args) {
        Request<T> request = new Request<>(cmd, deserializer, args);
        for (int i = 0; i < MAX_RETRIES; i++) {
            if (requestQueue.offer(request)) {
                idleStrategy.reset();
                return request.getPromise();
            } else {
                idleStrategy.idle();
            }
        }
        throw new IllegalStateException("Queue is full");
    }
}
