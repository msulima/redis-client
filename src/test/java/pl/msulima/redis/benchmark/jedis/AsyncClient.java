package pl.msulima.redis.benchmark.jedis;

class AsyncClient implements Client {

    private final int setRatio;
    private final byte[][] keys;
    private final byte[][] values;
    private final JedisClient client;

    public AsyncClient(byte[][] keys, byte[][] values, int batchSize, int setRatio) {
        this.keys = keys;
        this.values = values;
        this.setRatio = setRatio;
        this.client = new JedisClient(batchSize);
    }

    public void run(int i, Runnable onComplete) {
        int k = i % keys.length;

        if (i % setRatio == 0) {
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
