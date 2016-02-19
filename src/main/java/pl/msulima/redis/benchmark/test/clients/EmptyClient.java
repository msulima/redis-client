package pl.msulima.redis.benchmark.test.clients;

import pl.msulima.redis.benchmark.test.OnResponse;
import pl.msulima.redis.benchmark.test.TestConfiguration;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public class EmptyClient implements Client {

    private static final int N_THREADS = 8;
    private final Executor pool = new ForkJoinPool(N_THREADS, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
    private final TestConfiguration configuration;

    public EmptyClient(TestConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void run(int i, OnResponse onComplete) {
        pool.execute(() -> {
            for (int j = 0; j < configuration.getBatchSize(); j++) {
                onComplete.requestFinished();
            }
        });
    }

    @Override
    public String name() {
        return "empty";
    }

    @Override
    public void close() throws IOException {
    }
}
