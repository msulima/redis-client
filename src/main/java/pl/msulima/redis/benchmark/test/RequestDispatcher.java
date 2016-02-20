package pl.msulima.redis.benchmark.test;

import org.HdrHistogram.Histogram;
import pl.msulima.redis.benchmark.test.clients.Client;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class RequestDispatcher {

    private static final int MAX_REQUESTS = 500_000;
    private final OnComplete[] requests;
    private final Semaphore semaphore = new Semaphore(0);
    private final int batchSize;

    private int counter;

    private final AtomicInteger active = new AtomicInteger();

    public RequestDispatcher(Client client, int batchSize, ConcurrentMap<Long, Histogram> histograms) {
        requests = new OnComplete[MAX_REQUESTS];
        this.batchSize = batchSize;

        for (int i = 0; i < requests.length; i++) {
            requests[i] = new OnComplete(client, semaphore, active, histograms, this.batchSize);
        }
    }

    public void execute(int requestId) {
        active.addAndGet(batchSize);
        OnComplete request = requests[counter % MAX_REQUESTS];
        counter++;
        request.prepare(requestId);
        request.run();
    }

    public int getActive() {
        return active.get();
    }

    public void awaitComplete() {
        try {
            semaphore.acquire(counter);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public int getSubmitted() {
        return counter * batchSize;
    }
}
