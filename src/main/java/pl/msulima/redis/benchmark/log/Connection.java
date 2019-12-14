package pl.msulima.redis.benchmark.log;

import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.protocol.Response;
import redis.clients.jedis.Protocol;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class Connection {

    private final ManyToOneConcurrentArrayQueue<Request<?>> requestQueue;

    public Connection(ManyToOneConcurrentArrayQueue<Request<?>> requestQueue) {
        this.requestQueue = requestQueue;
    }

    <T> CompletionStage<T> offer(Protocol.Command cmd, Function<Response, T> deserializer, byte[]... args) {
        Request<T> request = new Request<>(cmd, deserializer, args);
        requestQueue.offer(request);
        return request.getPromise();
    }
}
