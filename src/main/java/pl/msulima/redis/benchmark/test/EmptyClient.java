package pl.msulima.redis.benchmark.test;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class EmptyClient implements Client {

    private static final int N_THREADS = 600;
    private final Executor pool = Executors.newFixedThreadPool(N_THREADS);
    private final TestConfiguration configuration;

    public EmptyClient(TestConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void run(int i, Runnable onComplete) {
        pool.execute(() -> {
            for (int j = 0; j < configuration.getBatchSize(); j++) {
                onComplete.run();
            }
        });
    }

    @Override
    public String name() {
        return "empty";
    }
}
