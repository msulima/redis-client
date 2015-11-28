package pl.msulima.redis.benchmark.test;

import com.google.common.util.concurrent.RateLimiter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class SyncTestClient implements Client {

    private static final RateLimiter connectionLimiter = RateLimiter.create(50);

    private final ThreadLocal<Jedis> jedisPool;
    private final Executor pool;
    private final TestConfiguration configuration;

    public SyncTestClient(TestConfiguration configuration) {
        this.configuration = configuration;
        this.pool = Executors.newFixedThreadPool(configuration.getConcurrency());
        this.jedisPool = ThreadLocal.withInitial(() -> {
            connectionLimiter.acquire();
            return new Jedis(configuration.getHost());
        });
    }

    public void run(int i, Runnable onComplete) {
        pool.execute(() -> {
            Jedis jedis = jedisPool.get();
            if (configuration.getBatchSize() > 1) {
                Pipeline pipeline = jedis.pipelined();
                for (int j = 0; j < configuration.getBatchSize(); j++) {
                    runSingle(jedis, i + j);
                }
                pipeline.sync();
                for (int j = 0; j < configuration.getBatchSize(); j++) {
                    onComplete.run();
                }
            } else {
                runSingle(jedis, i);
                onComplete.run();
            }
        });
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
        return "sync";
    }
}
