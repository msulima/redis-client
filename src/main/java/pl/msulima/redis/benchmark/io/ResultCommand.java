package pl.msulima.redis.benchmark.io;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;

import java.util.concurrent.CompletableFuture;

public class ResultCommand<T> implements Command {

    private final Protocol.Command command;
    private final byte[][] arguments;
    private final CompletableFuture<T> callback;

    public ResultCommand(Protocol.Command command, CompletableFuture<T> callback, byte[]... arguments) {
        this.command = command;
        this.arguments = arguments;
        this.callback = callback;
    }

    @Override
    public void writeTo(RedisOutputStream outputStream) {
        Protocol.sendCommand(outputStream, command, arguments);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFrom(RedisInputStream inputStream) {
        try {
            callback.complete((T) Protocol.read(inputStream));
        } catch (JedisDataException ex) {
            callback.completeExceptionally(ex);
        }
    }
}
