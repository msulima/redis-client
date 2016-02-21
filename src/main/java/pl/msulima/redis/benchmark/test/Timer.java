package pl.msulima.redis.benchmark.test;

import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class Timer implements Runnable {

    private final int duration;
    private final Consumer<Long> consumer;

    private long requestId = 0;

    public Timer(int duration, Consumer<Long> consumer) {
        this.duration = duration;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        CountDownLatch countDownLatch = new CountDownLatch(duration);
        java.util.Timer timer = new java.util.Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                consumer.accept(requestId);
                requestId++;
                countDownLatch.countDown();
            }
        }, 0, 1);

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            timer.cancel();
        }
    }
}
