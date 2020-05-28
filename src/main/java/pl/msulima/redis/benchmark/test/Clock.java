package pl.msulima.redis.benchmark.test;

import java.util.concurrent.TimeUnit;

class Clock {

    private final int warmupPeriod;
    private final int maxRate;
    private final long startTimestamp;

    Clock(int warmupPeriod, int maxRate) {
        this.warmupPeriod = warmupPeriod;
        this.maxRate = maxRate;
        this.startTimestamp = System.nanoTime();
    }

    long microTime() {
        return TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
    }

    int currentRate() {
        long secondsPassed = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTimestamp) + 1;

        if (secondsPassed < warmupPeriod) {
            return (int) (maxRate * secondsPassed / warmupPeriod);
        } else {
            return maxRate;
        }
    }
}
