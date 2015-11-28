package pl.msulima.redis.benchmark.test;

import com.google.common.util.concurrent.RateLimiter;
import pl.msulima.redis.benchmark.jedis.FixedLatency;
import redis.clients.jedis.Jedis;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

public class SyncTestClient implements Client {

    private static final int N_THREADS = 500;
    private static final RateLimiter limiter = RateLimiter.create(50);

    private final int setRatio;
    private final byte[][] keys;
    private final byte[][] values;
    private final ThreadLocal<Jedis> jedis;
    private final Executor pool = Executors.newFixedThreadPool(N_THREADS);

    public SyncTestClient(String host, byte[][] keys, byte[][] values, int setRatio) {
        this.jedis = ThreadLocal.withInitial(() -> {
            limiter.acquire();
            return new Jedis(host);
        });
        this.keys = keys;
        this.values = values;
        this.setRatio = setRatio;
    }

    public void run(int i, Runnable onComplete) {
        pool.execute(() -> {
            int k = i % keys.length;

            if (ThreadLocalRandom.current().nextInt() % setRatio == 0) {
                jedis.get().set(keys[k], values[k]);
            } else {
                jedis.get().get(keys[k]);
            }

            FixedLatency.fixedLatency();

            onComplete.run();
        });
    }

    @Override
    public String name() {
        return "sync";
    }
}
