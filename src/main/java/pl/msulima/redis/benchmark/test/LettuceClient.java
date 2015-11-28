package pl.msulima.redis.benchmark.test;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import com.lambdaworks.redis.codec.ByteArrayCodec;

class LettuceClient implements Client {

    private final RedisAsyncCommands<byte[], byte[]> connection;
    private final TestConfiguration configuration;

    public LettuceClient(TestConfiguration configuration) {
        this.configuration = configuration;
        RedisClient client = RedisClient.create("redis://" + configuration.getHost());
        this.connection = client.connect(new ByteArrayCodec()).async();
    }

    public void run(int i, Runnable onComplete) {
        for (int j = 0; j < configuration.getBatchSize(); j++) {
            runSingle(i + j, onComplete);
        }
    }

    private void runSingle(int i, Runnable onComplete) {
        if (configuration.isPing()) {
            connection.ping().thenRun(onComplete::run);
        } else if (configuration.isSet()) {
            connection.set(configuration.getKey(i), configuration.getValue(i)).thenRun(onComplete::run);
        } else {
            connection.get(configuration.getKey(i)).thenAccept(bytes -> {
                onComplete.run();
            });
        }
    }

    @Override
    public String name() {
        return "lettuce";
    }
}
