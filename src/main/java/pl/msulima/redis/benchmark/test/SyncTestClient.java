package pl.msulima.redis.benchmark.test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("Duplicates")
public class SyncTestClient implements Client {

    private final JedisPool jedisPool;
    private final ExecutorService pool;
    private final TestConfiguration configuration;

    public SyncTestClient(TestConfiguration configuration) {
        this.configuration = configuration;
        this.pool = Executors.newFixedThreadPool(configuration.getConcurrency());
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(configuration.getConcurrency());
        poolConfig.setMaxTotal(configuration.getConcurrency());
        this.jedisPool = new JedisPool(configuration.getHost());
    }

    public void run(int i, Runnable onComplete) {
        pool.execute(() -> {
            try (Jedis jedis = jedisPool.getResource()) {
                if (configuration.getBatchSize() > 1) {
                    Pipeline pipeline = jedis.pipelined();
                    for (int j = 0; j < configuration.getBatchSize(); j++) {
                        runSingle(pipeline, i + j);
                    }
                    pipeline.sync();
                    for (int j = 0; j < configuration.getBatchSize(); j++) {
                        onComplete.run();
                    }
                } else {
                    runSingle(jedis, i);
                    onComplete.run();
                }
            }
        });
    }

    private void runSingle(Pipeline jedis, int i) {
        if (configuration.isPing()) {
            jedis.ping();
        } else if (configuration.isSet()) {
            jedis.set(configuration.getKey(i), configuration.getValue(i));
        } else {
            jedis.get(configuration.getKey(i));
        }
    }

    private void runSingle(Jedis jedis, int i) {
        if (configuration.isPing()) {
            jedis.ping();
        } else if (configuration.isSet()) {
            jedis.set(configuration.getKey(i), configuration.getValue(i));
        } else {
            jedis.get(configuration.getKey(i));
        }
    }

    @Override
    public String name() {
        return String.format("sync %d %d", configuration.getBatchSize(), configuration.getConcurrency());
    }

    @Override
    public void close() throws IOException {
        try {
            pool.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        pool.shutdown();
        jedisPool.close();
    }
}
