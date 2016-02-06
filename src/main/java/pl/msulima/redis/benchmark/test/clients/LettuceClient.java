package pl.msulima.redis.benchmark.test.clients;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnectionPool;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import com.lambdaworks.redis.codec.ByteArrayCodec;
import pl.msulima.redis.benchmark.test.TestConfiguration;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class LettuceClient implements Client {

    private final TestConfiguration configuration;
    private final ExecutorService pool = Executors.newFixedThreadPool(8);
    private final RedisConnectionPool<RedisAsyncCommands<byte[], byte[]>> connectionPool;

    public LettuceClient(TestConfiguration configuration) {
        this.configuration = configuration;
        RedisClient client = RedisClient.create("redis://" + configuration.getHost());
        this.connectionPool = client.asyncPool(new ByteArrayCodec(), configuration.getConcurrency(), configuration.getConcurrency());
    }

    public void run(int i, Runnable onComplete) {
        pool.execute(() -> {
            for (int j = 0; j < configuration.getBatchSize(); j++) {
                runSingle(i + j, onComplete);
            }
        });
    }

    private void runSingle(int i, Runnable onComplete) {
        RedisAsyncCommands<byte[], byte[]> connection = connectionPool.allocateConnection();
        if (configuration.isPing()) {
            connection.ping().thenRun(() -> {
                connectionPool.freeConnection(connection);
                onComplete.run();
            });
        } else if (configuration.isSet()) {
            connection.set(configuration.getKey(i), configuration.getValue(i)).thenRun(() -> {
                connectionPool.freeConnection(connection);
                onComplete.run();
            });
        } else {
            connection.get(configuration.getKey(i)).thenAccept(bytes -> {
                connectionPool.freeConnection(connection);
                onComplete.run();
            });
        }
    }

    @Override
    public String name() {
        return "lettuce";
    }

    @Override
    public void close() throws IOException {
        try {
            pool.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        pool.shutdown();
        connectionPool.close();
    }
}
