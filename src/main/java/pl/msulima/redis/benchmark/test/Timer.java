package pl.msulima.redis.benchmark.test;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

public class Timer {

    private static final int ADJUST_FREQUENCY = 20;
    private static final int ADJUST_PERIOD = 3000;

    private Queue<Long> lastResults;

    public static void main(String... args) {
        new Timer().run(60_000, (millisecondsPassed) -> {
            LockSupport.parkNanos(10);
        });
    }

    void run(long duration, Consumer<Long> runnable) {
        int pauseTime = 760_000;
        lastResults = new ArrayDeque<>();
        saveLastResults();

        for (long i = 0; i < duration; i++) {
            runnable.accept(i);

            if (i % ADJUST_FREQUENCY == 0) {
                pauseTime -= Math.max(Math.min(getMillisecondsPassed(), 100), -100);
                pauseTime = Math.max(pauseTime, 0);
                if (i % 1000 == 0) {
                    System.out.println(pauseTime);
                }
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
