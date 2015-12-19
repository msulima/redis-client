package pl.msulima.redis.benchmark.io;

import redis.clients.jedis.Protocol;

import java.util.function.Consumer;

public class CommandHolder<T> {

    private Protocol.Command command;
    private byte[][] arguments;
    private Consumer<? extends T> callback;
    private Consumer<Throwable> onError;

    public CommandHolder() {
    }

    public CommandHolder(Protocol.Command command, Consumer<T> callback, Consumer<Throwable> onError, byte[][] arguments) {
        this.command = command;
        this.callback = callback;
        this.onError = onError;
        this.arguments = arguments;
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

    public Consumer<? extends T> getCallback() {
        return callback;
    }

    public void setCallback(Consumer<? extends T> callback) {
        this.callback = callback;
    }

    public void setOnError(Consumer<Throwable> onError) {
        this.onError = onError;
    }

    public Consumer<? extends Throwable> getOnError() {
        return onError;
    }
}
