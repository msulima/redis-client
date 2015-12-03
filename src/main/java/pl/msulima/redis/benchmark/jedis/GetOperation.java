package pl.msulima.redis.benchmark.jedis;

import pl.msulima.redis.benchmark.nio.Writer;
import redis.clients.jedis.Pipeline;
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
    public void run(Pipeline jedis) {
        response = jedis.get(key);
    }

    @Override
    public byte[] getBytes() {
        return ("GET " + new String(key) + "\r\n").getBytes();
    }

    @Override
    public void writeTo(ByteBuffer byteBuffer) {
        Writer.sendCommand(byteBuffer, "GET", key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void done() {
        done(response.get());
    }

    @Override
    public void done(Object response) {
        callback.accept(((Optional<byte[]>) response).orElse(null));
    }
}
