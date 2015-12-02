package pl.msulima.redis.benchmark.jedis;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

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

    @SuppressWarnings("unchecked")
    @Override
    public void done() {
        done(response.get());
    }

    @Override
    public void done(Object response) {
        callback.accept((byte[]) response);
    }
}
