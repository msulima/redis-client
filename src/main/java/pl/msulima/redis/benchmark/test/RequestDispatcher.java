package pl.msulima.redis.benchmark.test;

import org.HdrHistogram.Histogram;
import pl.msulima.redis.benchmark.test.clients.Client;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RequestDispatcher {

    public static final long HIGHEST_TRACKABLE_VALUE = 10_000_000L;
    public static final int NUMBER_OF_SIGNIFICANT_VALUE_DIGITS = 4;
    private static final int MAX_REQUESTS = 500_000;
    private final ExecutorService executorService = new ForkJoinPool(8, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
    private final OnComplete[] requests;
    private final Semaphore semaphore = new Semaphore(0);
    private final int batchSize;

    private int submitted;

    private final AtomicInteger active = new AtomicInteger();
    private final ConcurrentMap<Long, Histogram> histograms;

    public RequestDispatcher(Client client, int batchSize) {
        requests = new OnComplete[MAX_REQUESTS];
        histograms = new ConcurrentHashMap<>();
        this.batchSize = batchSize;

        for (int i = 0; i < requests.length; i++) {
            requests[i] = new OnComplete(client, semaphore, active, histograms, this.batchSize);
        }
    }

    public void execute(int requestId) {
        active.addAndGet(batchSize);
        OnComplete request = requests[submitted % MAX_REQUESTS];
        submitted++;
        request.prepare(requestId);
        executorService.execute(request);
    }

    public Histogram histograms() {
        Histogram histogram = new Histogram(HIGHEST_TRACKABLE_VALUE, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);
        histograms.values().forEach(histogram::add);
        return histogram;
    }

    public int getActive() {
        return active.get();
    }

    public void awaitComplete() {
        try {
            semaphore.acquire(submitted);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
