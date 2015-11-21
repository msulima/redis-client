package pl.msulima.redis.benchmark.test;

import pl.msulima.redis.benchmark.jedis.FixedLatency;
import redis.clients.jedis.Jedis;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class SyncTestClient implements Client {

    private static final int N_THREADS = 124;

    private final int setRatio;
    private final byte[][] keys;
    private final byte[][] values;
    private final ThreadLocal<Jedis> jedis;
    private final Executor pool = Executors.newFixedThreadPool(N_THREADS);

    public SyncTestClient(String host, byte[][] keys, byte[][] values, int setRatio) {
        this.jedis = ThreadLocal.withInitial(() -> new Jedis(host));
        this.keys = keys;
        this.values = values;
        this.setRatio = setRatio;
    }

    public void run(int i, Runnable onComplete) {
        pool.execute(() -> {
            int k = i % keys.length;

            if (i % setRatio == 0) {
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
