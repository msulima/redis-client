package pl.msulima.redis.benchmark.test;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;

import java.util.concurrent.TimeUnit;

public class Scheduler implements Runnable {

    private final int duration;
    private final Runnable consumer;

    private final IdleStrategy idleStrategy = new SleepingIdleStrategy(TimeUnit.MICROSECONDS.toNanos(100));

    public Scheduler(int duration, Runnable consumer) {
        this.duration = duration;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        long millisecondsPassed;
        do {
            millisecondsPassed = System.currentTimeMillis() - startTime;
            consumer.run();
            idleStrategy.idle();
        } while (millisecondsPassed < duration);
    }
}
