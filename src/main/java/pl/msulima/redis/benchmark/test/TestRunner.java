package pl.msulima.redis.benchmark.test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class TestRunner implements Runnable {

    private final Client client;
    private final int repeats;
    private final int throughput;
    private final int batchSize;
    private final Timer meter;
    private final Counter active;

    public TestRunner(Client client, int repeats, int throughput, int batchSize, Timer meter, Counter active) {
        this.client = client;
        this.repeats = repeats;
        this.throughput = throughput;
        this.batchSize = batchSize;
        this.meter = meter;
        this.active = active;
    }

    public void run() {
        CountDownLatch latch = new CountDownLatch(repeats);

        int perMillisecond = throughput / batchSize / 1000;
        int pauseTime = 760_000;

        long start = System.nanoTime();

        for (int i = 0, j = 0; i < repeats; i = i + batchSize, j++) {
            active.inc(batchSize);
            client.run(i, new OnComplete(latch, meter, active));

            if (j % perMillisecond == 0) {
                long millisecondsPassed = (System.nanoTime() - start) / 1_000_000;
                if (millisecondsPassed > 0 && j / millisecondsPassed > perMillisecond) {
                    pauseTime += 10;
                } else {
                    pauseTime -= 10;
                }

                LockSupport.parkNanos(pauseTime);
            }
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("done");
    }

    public static class OnComplete implements Runnable {

        private final CountDownLatch latch;
        private final Timer meter;
        private final Counter active;
        private final long start;

        public OnComplete(CountDownLatch latch, Timer meter, Counter active) {
            this.latch = latch;
            this.meter = meter;
            this.active = active;
            this.start = System.nanoTime();
        }

        @Override
        public void run() {
            meter.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            active.dec();

            latch.countDown();
        }
    }
}
