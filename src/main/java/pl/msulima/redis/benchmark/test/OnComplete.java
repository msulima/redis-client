package pl.msulima.redis.benchmark.test;

import org.HdrHistogram.Histogram;
import pl.msulima.redis.benchmark.test.clients.Client;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class OnComplete implements Runnable, OnResponse {

    private final Client client;
    private final Semaphore semaphore;
    private final AtomicInteger active;
    private final ConcurrentMap<Long, Histogram> histograms;
    private final int batchSize;
    private final AtomicInteger leftInBatch = new AtomicInteger();

    private volatile long start;
    private volatile int requestId;

    public OnComplete(Client client, Semaphore semaphore, AtomicInteger active,
                      ConcurrentMap<Long, Histogram> histograms, int batchSize) {
        this.client = client;
        this.semaphore = semaphore;
        this.active = active;
        this.histograms = histograms;
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
        this.start = System.nanoTime();
        client.run(requestId, this);
    }

    @Override
    public void requestFinished() {
        long responseTime = System.nanoTime() - start;
        Histogram histogram = histograms.computeIfAbsent(Thread.currentThread().getId(),
                (k) -> new Histogram(RequestDispatcher.HIGHEST_TRACKABLE_VALUE, RequestDispatcher.NUMBER_OF_SIGNIFICANT_VALUE_DIGITS));
        histogram.recordValue(responseTime / 1000);

        if (leftInBatch.decrementAndGet() == 0) {
            active.addAndGet(-batchSize);
            semaphore.release();
        }
    }
}
