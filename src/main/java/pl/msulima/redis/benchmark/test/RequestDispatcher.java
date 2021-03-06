package pl.msulima.redis.benchmark.test;

import pl.msulima.redis.benchmark.test.clients.Client;
import pl.msulima.redis.benchmark.test.metrics.MetricsRegistry;

import java.util.concurrent.atomic.AtomicInteger;

public class RequestDispatcher {

    private static final int MAX_REQUESTS = 500_000;
    private final OnResponse[] requests;
    private final AtomicInteger done = new AtomicInteger();
    private final int batchSize;

    private int counter;

    private final AtomicInteger active = new AtomicInteger();

    public RequestDispatcher(Client client, int batchSize, MetricsRegistry metricsRegistry) {
        this.requests = new OnResponse[MAX_REQUESTS];
        this.batchSize = batchSize;

        for (int i = 0; i < this.requests.length; i++) {
            this.requests[i] = new OnResponse(client, this.done, this.active, this.batchSize, metricsRegistry);
        }
    }

    public void execute(int requestId) {
        active.addAndGet(batchSize);
        OnResponse request = requests[counter % MAX_REQUESTS];
        counter++;
        request.prepare(requestId);
        request.run();
    }

    public int getActive() {
        return active.get();
    }

    public void awaitComplete() {
        while (done.get() != getSubmitted()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public int getSubmitted() {
        return counter * batchSize;
    }
}
