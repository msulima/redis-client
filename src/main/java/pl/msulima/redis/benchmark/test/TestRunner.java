package pl.msulima.redis.benchmark.test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class TestRunner {

    private final Client client;
    private final int repeats;
    private final int throughput;
    private final int batchSize;
    private final Timer timer;
    private final Counter active;
    private final ExecutorService executorService = new ForkJoinPool(8, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);

    public TestRunner(Client client, int repeats, int throughput, int batchSize, Timer timer, Counter active) {
        this.client = client;
        this.repeats = repeats;
        this.throughput = throughput;
        this.batchSize = batchSize;
        this.timer = timer;
        this.active = active;
    }

    public boolean run() {
        CountDownLatch latch = new CountDownLatch(repeats);

        int pauseTime = 760_000;

        long start = System.nanoTime();
        int processedUntilNow = 0;

        for (long millisecondsPassed = 0; processedUntilNow < repeats; millisecondsPassed++) {
            long toProcess = (millisecondsPassed + 1) * getPerSecond(millisecondsPassed) / 1000;

            for (; processedUntilNow < toProcess; processedUntilNow = processedUntilNow + batchSize) {
                active.inc(batchSize);
                int x = processedUntilNow;
                executorService.execute(() -> client.run(x, new OnComplete(latch, timer, active)));
            }

            long actualMillisecondsPassed = (System.nanoTime() - start) / 1_000_000;
            if (actualMillisecondsPassed < millisecondsPassed) {
                pauseTime += 10;
            } else {
                pauseTime -= 10;
            }

            if (pauseTime > 0) {
                LockSupport.parkNanos(pauseTime);
            }

            if (active.getCount() > throughput * 5) {
                System.out.println(active.getCount());
                return false;
            }
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("done");
        return true;
    }

    private long getPerSecond(long millisecondsPassed) {
        long secondsPassed = (millisecondsPassed / 1000) + 1;
        long x;
        if (secondsPassed < 15) {
            x = secondsPassed * 3;
        } else {
            x = Math.min(45 + secondsPassed, 100);
        }
        return throughput * x / 100;
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
