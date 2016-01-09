package pl.msulima.redis.benchmark.io;

import redis.clients.jedis.Protocol;

import java.util.function.BiConsumer;

public class CommandHolder<T> {

    private Protocol.Command command;
    private byte[][] arguments;
    private BiConsumer<byte[], Throwable> callback;

    public CommandHolder() {
    }

    public Protocol.Command getCommand() {
        return command;
    }

    public void setCommand(Protocol.Command command) {
        this.command = command;
    }

    public byte[][] getArguments() {
        return arguments;
    }

    public void setArguments(byte[][] arguments) {
        this.arguments = arguments;
    }

    public BiConsumer<byte[], Throwable> getCallback() {
        return callback;
    }

    public void setCallback(BiConsumer<byte[], Throwable> callback) {
        this.callback = callback;
    }
}
