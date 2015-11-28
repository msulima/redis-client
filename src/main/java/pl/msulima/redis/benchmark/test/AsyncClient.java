package pl.msulima.redis.benchmark.test;

import pl.msulima.redis.benchmark.jedis.JedisClient;

import java.util.concurrent.ThreadLocalRandom;

class AsyncClient implements Client {

    private final int setRatio;
    private final byte[][] keys;
    private final byte[][] values;
    private final JedisClient client;

    public AsyncClient(String host, byte[][] keys, byte[][] values, int batchSize, int setRatio) {
        this.keys = keys;
        this.values = values;
        this.setRatio = setRatio;
        this.client = new JedisClient(host, batchSize);
    }

    public void run(int i, Runnable onComplete) {
        int k = i % keys.length;

        if (ThreadLocalRandom.current().nextInt() % setRatio == 0) {
            client.set(keys[k], values[k], onComplete::run);
        } else {
            client.get(keys[k], bytes -> {
                onComplete.run();
                return null;
            });
        }
    }

    @Override
    public String name() {
        return "async";
    }
}
