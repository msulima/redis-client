package pl.msulima.redis.benchmark.jedis;

import redis.clients.jedis.Jedis;

public class SyncTestClient implements Client {

    private final int setRatio;
    private final byte[][] keys;
    private final byte[][] values;
    private final ThreadLocal<Jedis> jedis = ThreadLocal.withInitial(() -> new Jedis("localhost"));

    public SyncTestClient(byte[][] keys, byte[][] values, int setRatio) {
        this.keys = keys;
        this.values = values;
        this.setRatio = setRatio;
    }

    public void run(int i, Runnable onComplete) {
        int k = i % keys.length;

        if (i % setRatio == 0) {
            jedis.get().set(keys[k], values[k]);
        } else {
            jedis.get().get(keys[k]);
        }
        onComplete.run();
    }
}
