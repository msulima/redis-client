package pl.msulima.redis.benchmark.test;

import java.util.concurrent.TimeUnit;

class RateLimiter {

    private final int batchSize;
    private final Clock clock;

    private long startTimestamp;
    private int lastRate;
    private long counted;

    RateLimiter(Clock clock, int batchSize) {
        this.clock = clock;
        this.batchSize = batchSize;
        this.startTimestamp = clock.microTime();
        this.lastRate = clock.currentRate();
    }

    int calculateLimit() {
        long currentTimestamp = clock.microTime();
        long passedSinceStart = currentTimestamp - startTimestamp;

        int rate = clock.currentRate();
        if (rate != lastRate) {
            startTimestamp = currentTimestamp;
            counted = 0;
            lastRate = rate;
            return 0;
        }

        long expectedCount = TimeUnit.MICROSECONDS.toSeconds(lastRate * passedSinceStart);
        int idealDelta = (int) (expectedCount - counted);
        int delta = idealDelta / batchSize;
        counted += delta * batchSize;

        return delta;
    }
}
