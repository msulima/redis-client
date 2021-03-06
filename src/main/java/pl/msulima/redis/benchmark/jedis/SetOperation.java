package pl.msulima.redis.benchmark.jedis;

import redis.clients.jedis.Pipeline;

public class SetOperation implements Operation {

    private final byte[] key;
    private final byte[] value;
    private final Runnable callback;

    public SetOperation(byte[] key, byte[] value, Runnable callback) {
        this.key = key;
        this.value = value;
        this.callback = callback;
    }

    @Override
    public void run(Pipeline jedis) {
        jedis.set(key, value);
    }

    @Override
    public void done() {
        callback.run();
    }
}
