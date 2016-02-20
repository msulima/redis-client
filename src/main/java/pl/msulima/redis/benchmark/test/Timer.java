package pl.msulima.redis.benchmark.test;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

public class Timer implements Runnable {

    private static final int ADJUST_FREQUENCY = 20;
    private static final int ADJUST_PERIOD = 3000;
    private final long duration;
    private final Consumer<Long> consumer;

    private Queue<Long> lastResults;

    public Timer(long duration, Consumer<Long> consumer) {
        this.duration = duration;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        int pauseTime = 760_000;
        lastResults = new ArrayDeque<>();
        saveLastResults();

        for (long i = 0; i < duration; i++) {
            consumer.accept(i);

            if (i % ADJUST_FREQUENCY == 0) {
                pauseTime -= Math.max(Math.min(getMillisecondsPassed(), 20), -20);
                pauseTime = Math.max(pauseTime, 0);

                saveLastResults();
            }

            LockSupport.parkNanos(pauseTime);
        }
    }

    private void saveLastResults() {
        if (lastResults.size() == ADJUST_PERIOD / ADJUST_FREQUENCY) {
            lastResults.remove();
        }
        lastResults.add(System.nanoTime());
    }

    private long getMillisecondsPassed() {
        long expected = lastResults.size() * ADJUST_FREQUENCY;
        long passed = (System.nanoTime() - lastResults.peek()) / 1_000_000;
        return (passed - expected);
    }
}
