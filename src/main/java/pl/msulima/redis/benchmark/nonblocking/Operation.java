package pl.msulima.redis.benchmark.nonblocking;

import redis.clients.jedis.Protocol;

import java.util.function.Consumer;

public class Operation {

    private final byte[] command;
    private final byte[][] args;
    private final Consumer<Response> callback;

    public static Operation get(String key, Consumer<String> callback) {
        return new Operation(Protocol.Command.GET, (r) -> callback.accept(r.getString()), key.getBytes());
    }

    public static Operation set(String key, String value, Runnable callback) {
        return new Operation(Protocol.Command.SET, (r) -> callback.run(), key.getBytes(), value.getBytes());
    }

    private Operation(Protocol.Command command, Consumer<Response> callback, byte[]... args) {
        this.command = command.raw;
        this.callback = callback;
        this.args = args;
    }

    public byte[] command() {
        return command;
    }

    public byte[][] args() {
        return args;
    }

    public void finish(Response response) {
        callback.accept(response);
    }
}
