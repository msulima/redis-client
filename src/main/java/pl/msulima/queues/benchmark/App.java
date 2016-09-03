package pl.msulima.queues.benchmark;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class App {

    public static void main(String... args) {
        MetricRegistry metrics = createMetrics();

        int arrivalRate = 50;
        double averageServiceTime = 0.500;

        int L = (int) Math.ceil(Math.max(arrivalRate * averageServiceTime, 1));

        long arrivalPeriod = 1000 / arrivalRate;

        ExecutorService pool = Executors.newFixedThreadPool(100);
        Timer latencyTimer = metrics.timer("latency");
        Timer serviceTimer = metrics.timer("service");
        Timer responseTimer = metrics.timer("response");
        AtomicInteger inSystem = new AtomicInteger();

        java.util.Timer timer = new java.util.Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                metrics.histogram("inSystem").update(inSystem.get());
            }
        }, 0, 100);
        TaskFactory taskFactory = new TaskFactory(averageServiceTime, latencyTimer, serviceTimer, responseTimer, inSystem);

        long previous = 0;

        GammaDistributionGenerator distributionGenerator = new GammaDistributionGenerator(2, 10);

        for (int i = 0; i < 100_000; i++) {
            previous = previous + distributionGenerator.next();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    pool.submit(taskFactory.createTask());
                }
            }, previous);
        }
    }

    private static MetricRegistry createMetrics() {
        MetricRegistry metrics = new MetricRegistry();
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(5, TimeUnit.SECONDS);

        return metrics;
    }
}
