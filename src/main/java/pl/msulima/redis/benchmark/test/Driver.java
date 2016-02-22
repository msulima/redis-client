package pl.msulima.redis.benchmark.test;

public class Driver {

    private int processedUntilNow;
    private int processedAtStartOfSecond;
    private final RequestDispatcher requestDispatcher;
    private final int batchSize;
    private final int duration;
    private final int throughput;

    public Driver(RequestDispatcher requestDispatcher, int duration, int throughput, int batchSize) {
        this.requestDispatcher = requestDispatcher;
        this.duration = duration;
        this.throughput = throughput;
        this.batchSize = batchSize;
    }

    public void runMillisecond(long millisecondsPassed) {
        if (millisecondsPassed % 1000 == 0) {
            processedAtStartOfSecond = processedUntilNow;
        }
        double perMillisecond = getPerSecond(millisecondsPassed, duration, throughput);

        for (long shouldBeProcessed = Math.round((millisecondsPassed % 1000) * perMillisecond / 1000d) + processedAtStartOfSecond;
             processedUntilNow + batchSize < shouldBeProcessed;
             processedUntilNow += batchSize) {

            requestDispatcher.execute(processedUntilNow);
        }
    }

    public static long getPerSecond(long millisecondsPassed, int duration, int throughput) {
        long secondsPassed = (millisecondsPassed / 1000) + 1;
        int warmupPeriod = duration / 10;

        if (secondsPassed < warmupPeriod) {
            return throughput * secondsPassed / warmupPeriod;
        } else {
            return throughput;
        }
    }
}
