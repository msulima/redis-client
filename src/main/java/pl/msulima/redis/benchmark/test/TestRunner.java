package pl.msulima.redis.benchmark.test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class TestRunner {

    private final Client client;
    private final int repeats;
    private final int throughput;
    private final int batchSize;
    private final Timer meter;
    private final Counter active;
    private final ExecutorService executorService;

    public TestRunner(Client client, int repeats, int throughput, int batchSize, Timer meter, Counter active) {
        this.client = client;
        this.repeats = repeats;
        this.throughput = throughput;
        this.batchSize = batchSize;
        this.meter = meter;
        this.active = active;
        executorService = Executors.newFixedThreadPool(16);
    }

    public boolean run() {
        CountDownLatch latch = new CountDownLatch(repeats);

        int perSecond = throughput;
        int pauseTime = 760_000;

        long start = System.nanoTime();
        int processedUntilNow = 0;

        for (long millisecondsPassed = 0; processedUntilNow < repeats; millisecondsPassed++) {
            long toProcess = (millisecondsPassed + 1) * perSecond / 1000;

            for (; processedUntilNow < toProcess; processedUntilNow = processedUntilNow + batchSize) {
                active.inc(batchSize);
                int x = processedUntilNow;
                executorService.execute(() -> client.run(x, new OnComplete(latch, meter, active)));
            }

            long actualMillisecondsPassed = (System.nanoTime() - start) / 1_000_000;
            if (actualMillisecondsPassed < millisecondsPassed) {
                pauseTime += 10;
            } else {
                pauseTime -= 10;
            }

            LockSupport.parkNanos(pauseTime);

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
