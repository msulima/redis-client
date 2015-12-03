package pl.msulima.redis.benchmark.nio;

import redis.clients.jedis.Response;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Consumer;

public class GetOperation implements Operation {

    private final byte[] key;
    private final Consumer<byte[]> callback;
    private Response<byte[]> response;

    public GetOperation(byte[] key, Consumer<byte[]> callback) {
        this.key = key;
        this.callback = callback;
    }

    @Override
    public void writeTo(ByteBuffer byteBuffer) {
        Writer.sendCommand(byteBuffer, "GET", key);
    }

    @Override
    public void done(Object response) {
        callback.accept(((Optional<byte[]>) response).orElse(null));
    }
}
