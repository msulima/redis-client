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

    @SuppressWarnings("unchecked")
    @Override
    public void done() {
        callback.accept(response.get());
    }
}
