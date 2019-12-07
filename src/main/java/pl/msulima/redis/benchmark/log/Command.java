package pl.msulima.redis.benchmark.log;

import pl.msulima.redis.benchmark.log.protocol.Response;
import redis.clients.jedis.Protocol;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class Command<T> {

    public final Protocol.Command command;
    private final Function<Response, T> deserializer;
    public final byte[][] args;

    private final CompletableFuture<T> promise = new CompletableFuture<>();

    public Command(Protocol.Command command, Function<Response, T> deserializer, byte[]... args) {
        this.command = command;
        this.deserializer = deserializer;
        this.args = args;
    }

    void complete(Response response) {
        promise.complete(deserializer.apply(response));
    }

    public CompletionStage<T> getPromise() {
        return promise;
    }

    public static String decodeSimpleString(Response response) {
        return response.simpleString;
    }
}
