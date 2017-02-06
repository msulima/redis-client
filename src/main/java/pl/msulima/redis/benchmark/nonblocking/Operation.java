package pl.msulima.redis.benchmark.nonblocking;

import com.google.common.base.Charsets;
import redis.clients.jedis.Protocol;

import java.util.function.Consumer;

public class Operation {

    private Protocol.Command command;
    private byte[][] args;
    private Consumer<Response> callback;

    public static Operation get(String key, Consumer<byte[]> callback) {
        return get(key.getBytes(), callback);
    }

    public static Operation get(byte[] bytes, Consumer<byte[]> callback) {
        return new Operation(Protocol.Command.GET, r -> {
            if (r.isNull()) {
                callback.accept(null);
            } else {
                callback.accept(r.getBulkString());
            }
        }, bytes);
    }

    public static Operation set(String key, String value, Runnable callback) {
        return set(key.getBytes(Charsets.US_ASCII), value.getBytes(Charsets.US_ASCII), callback);
    }

    public static Operation set(byte[] key, byte[] value, Runnable callback) {
        return new Operation(Protocol.Command.SET, (r) -> callback.run(), key, value);
    }

    public Operation() {
    }

    private Operation(Protocol.Command command, Consumer<Response> callback, byte[]... args) {
        this.command = command;
        this.callback = callback;
        this.args = args;
    }

    public Protocol.Command command() {
        return command;
    }

    public byte[][] args() {
        return args;
    }

    public void finish(Response response) {
        callback.accept(response);
    }
}
