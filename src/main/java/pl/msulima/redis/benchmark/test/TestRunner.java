package pl.msulima.redis.benchmark.test;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import pl.msulima.redis.benchmark.test.clients.Client;

import java.util.Locale;
import java.util.TimerTask;

public class TestRunner {

    public static final long HIGHEST_TRACKABLE_VALUE = 10_000_000L;
    public static final int NUMBER_OF_SIGNIFICANT_VALUE_DIGITS = 4;
    private static final int PRINT_HISTOGRAM_RATE = 3000;
    private static final int THREADS = 4;

    private final ConcurrentHistogram histogram = new ConcurrentHistogram(HIGHEST_TRACKABLE_VALUE, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);
    private final RequestDispatcher[] requestDispatchers = new RequestDispatcher[THREADS];
    private final String name;
    private final int duration;
    private final int throughput;
    private final int batchSize;

    private long lastActualMillisecondsPassed;
    private long lastProcessedUntilNow;

    public TestRunner(Client client, String name, int duration, int throughput, int batchSize) {
        this.name = name;
        this.duration = duration;
        this.throughput = throughput;
        this.batchSize = batchSize;

        for (int i = 0; i < requestDispatchers.length; i++) {
            RequestDispatcher requestDispatcher = new RequestDispatcher(client, batchSize, histogram);
            requestDispatchers[i] = requestDispatcher;
        }
    }

    public void run() {
        boolean result = tryRun();

        for (RequestDispatcher requestDispatcher : requestDispatchers) {
            requestDispatcher.awaitComplete();
        }

        if (result) {
            printSummary();
        } else {
            printFailure();
        }
    }

    private boolean tryRun() {
        Thread[] threads = new Thread[requestDispatchers.length];
        java.util.Timer printTimer = new java.util.Timer();
        long start = System.nanoTime();

        printTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                printHistogram(start);
            }
        }, PRINT_HISTOGRAM_RATE, PRINT_HISTOGRAM_RATE);

        for (int i = 0; i < threads.length; i++) {
            Driver driver = new Driver(requestDispatchers[i], duration, throughput / THREADS, batchSize);

            Thread thread = new Thread(new Timer(duration * 1000, driver::runMillisecond));
            threads[i] = thread;
            thread.start();
        }

        try {
            for (Thread thread : threads) {
                thread.join();
            }
            printTimer.cancel();
            return true;
        } catch (InterruptedException | RuntimeException re) {
            re.printStackTrace();
            return false;
        }
    }


    private void printHistogram(long start) {
        Histogram histogram = this.histogram.copy();

        long active = 0;
        long processedUntilNow = 0;

        for (RequestDispatcher requestDispatcher : requestDispatchers) {
            int i = requestDispatcher.getActive();
            active += i;
            processedUntilNow += requestDispatcher.getSubmitted() - i;
        }

        long actualMillisecondsPassed = (System.nanoTime() - start) / 1_000_000;

        System.out.printf("--- %s %d %d/%d ---%n", name, throughput, actualMillisecondsPassed / 1000, duration);
        printPercentile(histogram, 50);
        printPercentile(histogram, 75);
        printPercentile(histogram, 95);
        printPercentile(histogram, 99);
        printPercentile(histogram, 99.9);
        printPercentile(histogram, 100);
        System.out.printf("mean       %.3f%n", histogram.getMean() / 1000d);
        System.out.println("done       " + processedUntilNow);
        System.out.println("active     " + active);
        long rate = (1000 * (processedUntilNow - lastProcessedUntilNow)) / (actualMillisecondsPassed - lastActualMillisecondsPassed);
        System.out.println("rate       " + rate);
        System.out.println("throughput " + Driver.getPerSecond(actualMillisecondsPassed, duration, throughput));
        lastActualMillisecondsPassed = actualMillisecondsPassed;
        lastProcessedUntilNow = processedUntilNow;
    }

    private void printPercentile(Histogram histogram, double percentile) {
        double v = histogram.getValueAtPercentile(percentile) / 1000d;
        System.out.printf("%5.1f%% %.3f\n", percentile, v);
    }

    private void printSummary() {
        Histogram histogram = this.histogram.copy();
        System.out.printf(Locale.forLanguageTag("PL"), "SUMMARY\tOK\t%s\t%d\t%.3f\t%.3f%n", name, throughput,
                histogram.getMean() / 1000d, histogram.getValueAtPercentile(99.99) / 1000d);
    }

    private void printFailure() {
        System.out.printf("SUMMARY\tKO\t%s\t%d0\t0%n", name, throughput);
    }
}
