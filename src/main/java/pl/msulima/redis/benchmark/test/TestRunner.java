package pl.msulima.redis.benchmark.test;

import org.HdrHistogram.Histogram;
import pl.msulima.redis.benchmark.test.clients.Client;

import java.util.Locale;
import java.util.function.Consumer;

public class TestRunner {

    private static final int PRINT_HISTOGRAM_RATE = 1000;

    private final String name;
    private final int duration;
    private final int throughput;
    private final int batchSize;
    private long lastActualMillisecondsPassed;
    private int lastProcessedUntilNow;
    private int processedUntilNow;
    private int processedAtStartOfSecond;
    private final RequestDispatcher requestDispatcher;

    public TestRunner(Client client, String name, int duration, int throughput, int batchSize) {
        this.name = name;
        this.duration = duration;
        this.throughput = throughput;
        this.batchSize = batchSize;
        requestDispatcher = new RequestDispatcher(client, batchSize);
    }

    public void run() {
        boolean result = tryRun();

        requestDispatcher.awaitComplete();

        if (result) {
            printSummary();
        } else {
            printFailure();
        }
    }

    private boolean tryRun() {
        Timer timer = new Timer();
        long start = System.nanoTime();
        try {
            timer.run(duration * 1000, runMillisecond(start));

            return true;
        } catch (RuntimeException re) {
            re.printStackTrace();
            return false;
        }
    }

    private Consumer<Long> runMillisecond(long start) {
        return (millisecondsPassed) -> {
            if (millisecondsPassed % 1000 == 0) {
                processedAtStartOfSecond = processedUntilNow;
            }
            double perMillisecond = getPerSecond(millisecondsPassed) / 1000;

            for (long shouldBeProcessed = Math.round((millisecondsPassed % 1000) * perMillisecond) + processedAtStartOfSecond;
                 processedUntilNow + batchSize < shouldBeProcessed;
                 processedUntilNow += batchSize) {

                requestDispatcher.execute(processedUntilNow);
            }

            if (millisecondsPassed % PRINT_HISTOGRAM_RATE == 0 && millisecondsPassed >= PRINT_HISTOGRAM_RATE) {
                long actualMillisecondsPassed = (System.nanoTime() - start) / 1_000_000;
                printHistogram(actualMillisecondsPassed, processedUntilNow);
            }
        };
    }

    private long getPerSecond(long millisecondsPassed) {
        long secondsPassed = (millisecondsPassed / 1000) + 1;
        int warmupPeriod = duration / 10;

        if (secondsPassed < warmupPeriod) {
            return throughput * secondsPassed / warmupPeriod;
        } else {
            return throughput;
        }
    }

    private void printHistogram(long actualMillisecondsPassed, int processedUntilNow) {
        Histogram histogram = requestDispatcher.histograms();
        System.out.printf("--- %s %d %d/%d ---%n", name, throughput, actualMillisecondsPassed / 1000, duration);
        printPercentile(histogram, 50);
        printPercentile(histogram, 75);
        printPercentile(histogram, 95);
        printPercentile(histogram, 99);
        printPercentile(histogram, 99.9);
        printPercentile(histogram, 100);
        System.out.printf("mean %.3f%n", histogram.getMean() / 1000d);
        System.out.println("done   " + histogram.getTotalCount());
        System.out.println("active " + requestDispatcher.getActive());
        long rate = 1000 * (processedUntilNow - lastProcessedUntilNow) / (actualMillisecondsPassed - lastActualMillisecondsPassed);
        System.out.println("rate       " + rate);
        System.out.println("throughput " + getPerSecond(actualMillisecondsPassed));
        lastActualMillisecondsPassed = actualMillisecondsPassed;
        lastProcessedUntilNow = processedUntilNow;
    }

    private void printPercentile(Histogram histogram, double percentile) {
        double v = histogram.getValueAtPercentile(percentile) / 1000d;
        System.out.printf("%4.1f%% %.3f\n", percentile, v);
    }

    private void printSummary() {
        Histogram histogram = requestDispatcher.histograms();
        System.out.printf(Locale.forLanguageTag("PL"), "SUMMARY\tOK\t%s\t%d\t%.3f\t%.3f%n", name, throughput,
                histogram.getMean() / 1000d, histogram.getValueAtPercentile(99.99) / 1000d);
    }

    private void printFailure() {
        System.out.printf("SUMMARY\tKO\t%s\t%d0\t0%n", name, throughput);
    }

}
