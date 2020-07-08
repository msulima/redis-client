package pl.msulima.redis.benchmark.log;

import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.protocol.Command;
import pl.msulima.redis.benchmark.log.protocol.Response;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

public class Connection {

    private static final int MAX_RETRIES = 1024;

    private final ManyToOneConcurrentArrayQueue<Request<?>> requestQueue;

    public Connection(ManyToOneConcurrentArrayQueue<Request<?>> requestQueue) {
        this.requestQueue = requestQueue;
    }

    <T> CompletionStage<T> offer(Command cmd, Function<Response, T> deserializer, byte[]... args) {
        Request<T> request = new Request<>(cmd, deserializer, args);
        for (int i = 0; i < MAX_RETRIES; i++) {
            if (requestQueue.offer(request)) {
                return request.getPromise();
            } else {
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
            }
        }
        throw new IllegalStateException("Queue is full");
    }
}
