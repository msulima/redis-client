package pl.msulima.queues.benchmark;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class App {

    public static void main(String... args) {
        MetricRegistry metricRegistry = createMetrics();

        int arrivalRate = 50;
        double averageServiceTime = 0.500;

        int L = (int) Math.ceil(Math.max(arrivalRate * averageServiceTime, 1));
        System.out.println("L=" + L);

        long arrivalPeriod = 1000 / arrivalRate;

        run(metricRegistry, averageServiceTime, 26, new GammaDistributionGenerator(2, 10));
        run(metricRegistry, averageServiceTime, 27, new GammaDistributionGenerator(2, 10));
        run(metricRegistry, averageServiceTime, 30, new GammaDistributionGenerator(2, 10));
        run(metricRegistry, averageServiceTime, 100, new GammaDistributionGenerator(2, 10));
        run(metricRegistry, averageServiceTime, 26, new ConstantDistributionGenerator(arrivalPeriod));
        run(metricRegistry, averageServiceTime, 30, new ConstantDistributionGenerator(arrivalPeriod));
        run(metricRegistry, averageServiceTime, 100, new ConstantDistributionGenerator(arrivalPeriod));
    }

    private static void run(MetricRegistry metricRegistry, double averageServiceTime,
                            int nThreads, DistributionGenerator distributionGenerator) {
        ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        String name = "-" + nThreads + "-" + distributionGenerator;
        java.util.Timer timer = new java.util.Timer();
        Metrics metrics = new Metrics(metricRegistry, name, timer);

        TaskFactory taskFactory = new TaskFactory(averageServiceTime, metrics);

        long previous = 0;

        int tasks = 10_000;
        CountDownLatch latch = new CountDownLatch(tasks);
        for (int i = 0; i < tasks; i++) {
            previous = previous + distributionGenerator.next();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    pool.submit(taskFactory.createTask());
                    latch.countDown();
                }
            }, previous);
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        metrics.shutdown();
        pool.shutdown();
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
