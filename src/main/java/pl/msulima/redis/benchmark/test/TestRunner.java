package pl.msulima.redis.benchmark.test;

import org.HdrHistogram.Histogram;
import pl.msulima.redis.benchmark.test.clients.Client;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TestRunner {

    private static final long HIGHEST_TRACKABLE_VALUE = 10_000_000L;
    private static final int NUMBER_OF_SIGNIFICANT_VALUE_DIGITS = 4;
    private static final int PRINT_HISTOGRAM_RATE = 1000;

    private final Client client;
    private final String name;
    private final int duration;
    private final int throughput;
    private final int batchSize;
    private final AtomicInteger active = new AtomicInteger();
    private final ExecutorService executorService = new ForkJoinPool(8, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
    private final ConcurrentMap<Long, Histogram> histograms;
    private long lastActualMillisecondsPassed;
    private int lastProcessedUntilNow;
    private int processedUntilNow;
    private int processedAtStartOfSecond;
    private int submitted;

    public TestRunner(Client client, String name, int duration, int throughput, int batchSize) {
        this.client = client;
        this.name = name;
        this.duration = duration;
        this.throughput = throughput;
        this.batchSize = batchSize;
        histograms = new ConcurrentHashMap<>();
    }

    public boolean run() {
        Semaphore semaphore = new Semaphore(0);
        long start = System.nanoTime();
        submitted = 0;

        Timer timer = new Timer();
        timer.run(duration * 1000, (millisecondsPassed) -> {
            if (millisecondsPassed % 1000 == 0) {
                processedAtStartOfSecond = processedUntilNow;
            }
            double perMillisecond = getPerSecond(millisecondsPassed) / 1000;

            for (long shouldBeProcessed = Math.round((millisecondsPassed % 1000) * perMillisecond) + processedAtStartOfSecond;
                 processedUntilNow + batchSize < shouldBeProcessed;
                 processedUntilNow += batchSize) {
                active.addAndGet(batchSize);
                int x = processedUntilNow;

                submitted++;
                executorService.execute(() -> client.run(x, new OnComplete(semaphore, active, histograms)));
            }

            int localActive = active.get();
            if (localActive > throughput * 5) {
                throw new RuntimeException(Long.toString(localActive));
            }

            if (millisecondsPassed % PRINT_HISTOGRAM_RATE == 0 && millisecondsPassed >= PRINT_HISTOGRAM_RATE) {
                long actualMillisecondsPassed = (System.nanoTime() - start) / 1_000_000;
                printHistogram(localActive, actualMillisecondsPassed, processedUntilNow);
            }
        });

        try {
            semaphore.acquire(submitted);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        printHistogram(0, 0, 0);
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

    private void printHistogram(int active, long actualMillisecondsPassed, int processedUntilNow) {
        Histogram histogram = new Histogram(HIGHEST_TRACKABLE_VALUE, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);
        histograms.values().forEach(histogram::add);
        System.out.println("--- " + name + " ---");
        printPercentile(histogram, 50);
        printPercentile(histogram, 75);
        printPercentile(histogram, 90);
        printPercentile(histogram, 95);
        printPercentile(histogram, 99);
        printPercentile(histogram, 100);
        System.out.printf("mean %.3f%n", histogram.getMean());
        System.out.println("done   " + histogram.getTotalCount());
        System.out.println("active " + active);
        long rate = 1000 * (processedUntilNow - lastProcessedUntilNow) / (actualMillisecondsPassed - lastActualMillisecondsPassed);
        System.out.println("rate       " + rate);
        System.out.println("throughput " + getPerSecond(actualMillisecondsPassed));
        lastActualMillisecondsPassed = actualMillisecondsPassed;
        lastProcessedUntilNow = processedUntilNow;
    }

    private void printPercentile(Histogram histogram, int percentile) {
        double v = histogram.getValueAtPercentile(percentile) / 1000d;
        System.out.printf("%3d%% %.3f\n", percentile, v);
    }

    public static class OnComplete implements Runnable {

        private final Semaphore semaphore;
        private final AtomicInteger active;
        private final ConcurrentMap<Long, Histogram> histograms;
        private final long start;

        public OnComplete(Semaphore semaphore, AtomicInteger active, ConcurrentMap<Long, Histogram> histograms) {
            this.semaphore = semaphore;
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

            semaphore.release();
        }
    }
}
