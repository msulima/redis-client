package pl.msulima.redis.benchmark.test.clients;

import com.google.common.collect.Lists;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnectionPool;
import com.lambdaworks.redis.RedisFuture;
import com.lambdaworks.redis.api.async.RedisAsyncCommands;
import com.lambdaworks.redis.codec.ByteArrayCodec;
import pl.msulima.redis.benchmark.test.OnResponse;
import pl.msulima.redis.benchmark.test.TestConfiguration;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LettuceClient implements Client {

    private final TestConfiguration configuration;
    private final ExecutorService pool = Executors.newFixedThreadPool(8);
    private final RedisConnectionPool<RedisAsyncCommands<byte[], byte[]>> connectionPool;

    public LettuceClient(TestConfiguration configuration) {
        this.configuration = configuration;
        RedisClient client = RedisClient.create("redis://" + configuration.getRedisAddresses().get(0).getHost());
        this.connectionPool = client.asyncPool(new ByteArrayCodec(), configuration.getConcurrency(), configuration.getConcurrency());
    }

    @Override
    public void run(int i, OnResponse onResponse) {
        pool.execute(() -> {
            try (RedisAsyncCommands<byte[], byte[]> connection = connectionPool.allocateConnection()) {
                runSingle(i, onResponse, connection);
            }
        });
    }

    private void runSingle(int i, OnResponse onComplete, RedisAsyncCommands<byte[], byte[]> connection) {
        if (configuration.getBatchSize() > 1) {
            connection.setAutoFlushCommands(false);
        }
        List<CompletableFuture<byte[]>> futures = Lists.newArrayList();

        for (int j = 0; j < configuration.getBatchSize(); j++) {
            futures.add(runSingle(connection, i + j));
        }

        if (configuration.getBatchSize() > 1) {
            connection.flushCommands();
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).whenComplete((result, throwable) -> {
            for (int j = 0; j < configuration.getBatchSize(); j++) {
                onComplete.requestFinished();
            }
        });
    }

    private CompletableFuture<byte[]> runSingle(RedisAsyncCommands<byte[], byte[]> connection, int i) {
        if (configuration.isSet()) {
            RedisFuture<String> set = connection.set(configuration.getKey(i), configuration.getValue(i));
            return set.thenApply(x -> new byte[0]).toCompletableFuture();
        } else {
            return connection.get(configuration.getKey(i)).toCompletableFuture();
        }
    }

    @Override
    public String name() {
        return "lettuce";
    }

    @Override
    public void close() {
        pool.shutdown();
        try {
            pool.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        connectionPool.close();
    }
}
