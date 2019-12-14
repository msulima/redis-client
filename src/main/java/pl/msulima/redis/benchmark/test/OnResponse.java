package pl.msulima.redis.benchmark.test;

import org.HdrHistogram.ConcurrentHistogram;
import pl.msulima.redis.benchmark.test.clients.Client;

import java.util.concurrent.atomic.AtomicInteger;

public class OnResponse implements Runnable {

    private final Client client;
    private final AtomicInteger done;
    private final AtomicInteger active;
    private final int batchSize;
    private final AtomicInteger leftInBatch = new AtomicInteger();
    private final ConcurrentHistogram histogram;

    private volatile long start;
    private volatile int requestId;

    public OnResponse(Client client, AtomicInteger done, AtomicInteger active,
                      int batchSize, ConcurrentHistogram histogram) {
        this.histogram = histogram;
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
        if (requestId * batchSize % 1000 == 0) {
            this.start = System.nanoTime();
        }
        client.run(requestId, this);
    }

    public void requestFinished() {
        if (leftInBatch.decrementAndGet() == 0) {
            if (requestId * batchSize % 1000 == 0) {
                long responseTime = System.nanoTime() - start;

                for (int i = 0; i < batchSize; i++) {
                    histogram.recordValue(responseTime / 1000);
                }
            }

            active.addAndGet(-batchSize);
            done.addAndGet(batchSize);
        }
    }
}
