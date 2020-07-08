package pl.msulima.redis.benchmark.test;

import pl.msulima.redis.benchmark.test.clients.Client;
import pl.msulima.redis.benchmark.test.metrics.MetricsRegistry;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class OnResponse implements Runnable {

    private static final int MEASURE_SAMPLE = 1000;

    private final Client client;
    private final AtomicInteger done;
    private final AtomicInteger active;
    private final int batchSize;
    private final AtomicInteger leftInBatch = new AtomicInteger();
    private final MetricsRegistry metricsRegistry;

    private volatile long start;
    private volatile int requestId;

    public OnResponse(Client client, AtomicInteger done, AtomicInteger active,
                      int batchSize, MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        this.client = client;
        this.done = done;
        this.active = active;
        this.batchSize = batchSize;
    }

    public void prepare(int requestId) {
        if (!leftInBatch.compareAndSet(0, batchSize)) {
            throw new IllegalStateException("Too many requests " + active.get());
        }
        this.requestId = requestId;
    }

    @Override
    public void run() {
        if (requestId * batchSize % MEASURE_SAMPLE == 0) {
            this.start = System.nanoTime();
        }
        client.run(requestId, this);
    }

    public void requestFinished() {
        if (leftInBatch.decrementAndGet() == 0) {
            if (requestId * batchSize % MEASURE_SAMPLE == 0) {
                long responseTime = System.nanoTime() - start;
                metricsRegistry.recordTime(responseTime, TimeUnit.NANOSECONDS, batchSize);
            }

            active.addAndGet(-batchSize);
            done.addAndGet(batchSize);
        }
    }
}
