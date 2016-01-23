package pl.msulima.redis.benchmark.test;

import org.HdrHistogram.Histogram;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class TestRunner {

    public static final long HIGHEST_TRACKABLE_VALUE = 10_000_000L;
    public static final int NUMBER_OF_SIGNIFICANT_VALUE_DIGITS = 4;
    private final Client client;
    private final int repeats;
    private final int throughput;
    private final int batchSize;
    private final AtomicInteger active = new AtomicInteger();
    private final ExecutorService executorService = new ForkJoinPool(8, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
    private final ConcurrentMap<Long, Histogram> histograms;

    public TestRunner(Client client, int repeats, int throughput, int batchSize) {
        this.client = client;
        this.repeats = repeats;
        this.throughput = throughput;
        this.batchSize = batchSize;
        histograms = new ConcurrentHashMap<>();
    }

    public boolean run() {
        CountDownLatch latch = new CountDownLatch(repeats);

        int pauseTime = 760_000;

        long start = System.nanoTime();
        int processedUntilNow = 0;

        for (long millisecondsPassed = 0; processedUntilNow < repeats; millisecondsPassed++) {
            long toProcess = (millisecondsPassed + 1) * getPerSecond(millisecondsPassed) / 1000;
            for (; processedUntilNow < toProcess; processedUntilNow = processedUntilNow + batchSize) {
                active.addAndGet(batchSize);
                int x = processedUntilNow;
                executorService.execute(() -> client.run(x, new OnComplete(latch, active, histograms)));
            }

            long actualMillisecondsPassed = (System.nanoTime() - start) / 1_000_000;
            if (actualMillisecondsPassed < millisecondsPassed) {
                pauseTime += 10;
            } else {
                pauseTime -= 10;
            }

            if (pauseTime > 0) {
                LockSupport.parkNanos(pauseTime);
            }

            int localActive = active.get();
            if (localActive > throughput * 5) {
                System.out.println(localActive);
                return false;
            }

            if (millisecondsPassed % 3000 == 0) {
                printHistogram(localActive);
            }
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("done");
        printHistogram(0);
        return true;
    }

    private long getPerSecond(long millisecondsPassed) {
        long secondsPassed = (millisecondsPassed / 1000) + 1;
        long x;
        if (secondsPassed < 15) {
            x = secondsPassed * 3;
        } else {
            x = Math.min(45 + secondsPassed, 100);
        }
        return throughput * x / 100;
    }

    private void printHistogram(int active) {
        Histogram histogram = new Histogram(HIGHEST_TRACKABLE_VALUE, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);
        histograms.values().forEach(histogram::add);
        histogram.outputPercentileDistribution(System.out, 1, 1000.0);
        System.out.println("active " + active);
    }

    public static class OnComplete implements Runnable {

        private final CountDownLatch latch;
        private final AtomicInteger active;
        private final ConcurrentMap<Long, Histogram> histograms;
        private final long start;

        public OnComplete(CountDownLatch latch, AtomicInteger active, ConcurrentMap<Long, Histogram> histograms) {
            this.latch = latch;
            this.active = active;
            this.histograms = histograms;
            this.start = System.nanoTime();
        }

        @Override
        public void run() {
            long responseTime = System.nanoTime() - start;
            active.decrementAndGet();
            Histogram histogram = histograms.computeIfAbsent(Thread.currentThread().getId(),
                    (k) -> new Histogram(HIGHEST_TRACKABLE_VALUE, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS));
            histogram.recordValue(responseTime / 1000);

            latch.countDown();
        }
    }
}
