package pl.msulima.redis.benchmark.test;

import pl.msulima.redis.benchmark.jedis.FixedLatency;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class EmptyClient implements Client {

    private static final int N_THREADS = 600;
    private final Executor pool = Executors.newFixedThreadPool(N_THREADS);

    @Override
    public void run(int i, Runnable onComplete) {
        pool.execute(() -> {
            FixedLatency.fixedLatency();
            onComplete.run();
        });
    }

    @Override
    public String name() {
        return "empty";
    }
}
