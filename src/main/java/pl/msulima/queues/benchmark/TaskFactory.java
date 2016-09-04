package pl.msulima.queues.benchmark;

import java.util.concurrent.TimeUnit;

public class TaskFactory {

    private double averageResponseTime;
    private final Metrics metrics;

    public TaskFactory(double averageResponseTime, Metrics metrics) {
        this.averageResponseTime = averageResponseTime;
        this.metrics = metrics;
    }

    public Runnable createTask() {
        long submit = System.currentTimeMillis();
        metrics.inSystem.incrementAndGet();

        return () -> {
            long start = System.currentTimeMillis();
            try {
                Thread.sleep((int) (this.averageResponseTime * 1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long finish = System.currentTimeMillis();

            metrics.inSystem.decrementAndGet();
            metrics.latencyTimer.update(start - submit, TimeUnit.MILLISECONDS);
            metrics.serviceTimer.update(finish - start, TimeUnit.MILLISECONDS);
            metrics.responseTimer.update(finish - submit, TimeUnit.MILLISECONDS);
        };
    }
}