package pl.msulima.queues.benchmark;

import com.codahale.metrics.Timer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskFactory {

    private double averageResponseTime;
    private Timer latencyTimer;
    private Timer serviceTimer;
    private Timer responseTimer;
    private AtomicInteger inSystem;

    public TaskFactory(double averageResponseTime, Timer latencyTimer, Timer serviceTimer, Timer responseTimer, AtomicInteger inSystem) {
        this.averageResponseTime = averageResponseTime;
        this.latencyTimer = latencyTimer;
        this.serviceTimer = serviceTimer;
        this.responseTimer = responseTimer;
        this.inSystem = inSystem;
    }

    public Runnable createTask() {
        long submit = System.currentTimeMillis();
        inSystem.incrementAndGet();

        return () -> {
            try {
                long start = System.currentTimeMillis();
                Thread.sleep((int) (this.averageResponseTime * 1000));
                long finish = System.currentTimeMillis();

                this.inSystem.decrementAndGet();
                this.latencyTimer.update(start - submit, TimeUnit.MILLISECONDS);
                this.serviceTimer.update(finish - start, TimeUnit.MILLISECONDS);
                this.responseTimer.update(finish - submit, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };
    }
}