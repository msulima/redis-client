package pl.msulima.redis.benchmark.jedis;

import redis.clients.jedis.Pipeline;

import java.util.function.Consumer;

public class DelOperation implements Operation {

    private final byte[] key;
    private final Consumer<Integer> callback;

    public DelOperation(byte[] key, Consumer<Integer> callback) {
        this.key = key;
        this.callback = callback;
    }

    @Override
    public void run(Pipeline jedis) {
        jedis.del(key);
    }

    @Override
    public void done() {
        callback.accept(1);
    }
}
