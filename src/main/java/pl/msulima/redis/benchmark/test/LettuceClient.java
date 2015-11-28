package pl.msulima.redis.benchmark.test;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import com.lambdaworks.redis.codec.ByteArrayCodec;

import java.util.concurrent.ThreadLocalRandom;

class LettuceClient implements Client {

    private final int setRatio;
    private final byte[][] keys;
    private final byte[][] values;
    private final RedisAsyncCommands<byte[], byte[]> connection;

    public LettuceClient(String host, byte[][] keys, byte[][] values, int setRatio) {
        this.keys = keys;
        this.values = values;
        this.setRatio = setRatio;
        RedisClient client = RedisClient.create("redis://" + host);
        this.connection = client.connect(new ByteArrayCodec()).async();
    }

    public void run(int i, Runnable onComplete) {
        int k = i % keys.length;

        if (ThreadLocalRandom.current().nextInt() % setRatio == 0) {
            connection.set(keys[k], values[k]).thenRun(onComplete::run);
        } else {
            connection.get(keys[k]).thenAccept(bytes -> {
                onComplete.run();
            });
        }
    }

    @Override
    public String name() {
        return "lettuce";
    }
}
