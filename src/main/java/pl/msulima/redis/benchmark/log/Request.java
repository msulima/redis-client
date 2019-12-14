package pl.msulima.redis.benchmark.log;

import pl.msulima.redis.benchmark.log.protocol.Command;
import pl.msulima.redis.benchmark.log.protocol.DynamicEncoder;
import pl.msulima.redis.benchmark.log.protocol.Response;

import java.util.Arrays;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public class Request<T> {

    public final Command command;
    private final Function<Response, T> deserializer;
    public final byte[][] args;

    private final CompletableFuture<T> promise = new CompletableFuture<>();

    public Request(Command command, Function<Response, T> deserializer, byte[]... args) {
        this.command = command;
        this.deserializer = deserializer;
        this.args = args;
    }

    public void complete(Response response) {
        promise.complete(deserializer.apply(response));
    }

    public CompletionStage<T> getPromise() {
        return promise;
    }

    public static String getSimpleString(Response response) {
        return response.simpleString;
    }

    public static String decodeBulkString(Response response) {
        return new String(response.bulkString, DynamicEncoder.CHARSET);
    }

    public static byte[] getBulkString(Response response) {
        return response.bulkString;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Request.class.getSimpleName() + "[", "]")
                .add("command=" + command)
                .add("deserializer=" + deserializer)
                .add("args=" + Arrays.toString(args))
                .add("promise=" + promise)
                .toString();
    }
}
