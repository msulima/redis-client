package pl.msulima.redis.benchmark.test;

public class Driver {

    private int processedUntilNow;
    private final RequestDispatcher requestDispatcher;
    private final int batchSize;
    private final RateLimiter rateLimiter;

    public Driver(RequestDispatcher requestDispatcher, int batchSize, Clock clock) {
        this.requestDispatcher = requestDispatcher;
        this.batchSize = batchSize;
        this.rateLimiter = new RateLimiter(clock, batchSize);
    }

    public void runMillisecond() {
        int toProcess = rateLimiter.calculateLimit();
        for (int i = 0; i < toProcess; i++) {
            requestDispatcher.execute(processedUntilNow);
            processedUntilNow += batchSize;
        }
    }
}
