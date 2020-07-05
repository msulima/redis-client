package pl.msulima.redis.benchmark.test.clients;

import pl.msulima.redis.benchmark.test.OnResponse;
import pl.msulima.redis.benchmark.test.TestConfiguration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.net.URI;
import java.util.concurrent.*;

@SuppressWarnings("Duplicates")
public class SyncThreadLocalClient implements Client {

    private final ThreadLocal<Jedis> jedis;
    private final ExecutorService pool;
    private final TestConfiguration configuration;
    private final ConcurrentMap<Long, Jedis> connections = new ConcurrentHashMap<>();

    public SyncThreadLocalClient(TestConfiguration configuration) {
        this.configuration = configuration;
        this.pool = Executors.newFixedThreadPool(configuration.getConcurrency());
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(configuration.getConcurrency());
        poolConfig.setMaxTotal(configuration.getConcurrency());

        jedis = ThreadLocal.withInitial(() -> {
            URI redisUri = configuration.getRedisAddresses().get(0);
            return new Jedis(redisUri.getHost(), redisUri.getPort());
        });
    }

    public void run(int i, OnResponse onComplete) {
        pool.execute(() -> {
            runWithConnection(i, onComplete, jedis.get());
        });
    }

    private void runWithConnection(int i, OnResponse onComplete, Jedis jedis) {
        if (configuration.getBatchSize() > 1) {
            Pipeline pipeline = jedis.pipelined();
            for (int j = 0; j < configuration.getBatchSize(); j++) {
                runSingle(pipeline, i + j);
            }
            pipeline.sync();
            for (int j = 0; j < configuration.getBatchSize(); j++) {
                onComplete.requestFinished();
            }
        } else {
            runSingle(jedis, i);
            onComplete.requestFinished();
        }
    }

    private void runSingle(Pipeline jedis, int i) {
        if (configuration.isSet()) {
            jedis.set(configuration.getKey(i), configuration.getValue(i));
        } else {
            jedis.get(configuration.getKey(i));
        }
    }

    private void runSingle(Jedis jedis, int i) {
        if (configuration.isSet()) {
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
    public void close() {
        pool.shutdown();
        try {
            pool.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        jedisPool.close();
    }
}
