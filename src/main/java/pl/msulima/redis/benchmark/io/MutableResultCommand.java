package pl.msulima.redis.benchmark.io;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;

import java.util.concurrent.CompletableFuture;

public class MutableResultCommand implements Command {

    private Protocol.Command command;
    private byte[][] arguments;
    private CompletableFuture callback;

    @Override
    public Protocol.Command getCommand() {
        return command;
    }

    @Override
    public byte[][] getArguments() {
        return arguments;
    }

    @Override
    public CompletableFuture getCallback() {
        return callback;
    }

    @Override
    public void writeTo(RedisOutputStream outputStream) {
        Protocol.sendCommand(outputStream, command, arguments);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFrom(RedisInputStream inputStream) {
        try {
            Object read = Protocol.read(inputStream);
            callback.complete(read);
        } catch (JedisDataException ex) {
            callback.completeExceptionally(ex);
        }
    }

    public void setCommand(Protocol.Command command) {
        this.command = command;
    }

    public void setArguments(byte[][] arguments) {
        this.arguments = arguments;
    }

    public void setCallback(CompletableFuture callback) {
        this.callback = callback;
    }
}
