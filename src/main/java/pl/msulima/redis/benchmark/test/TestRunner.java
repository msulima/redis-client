package pl.msulima.redis.benchmark.test;

import com.codahale.metrics.Timer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class TestRunner implements Runnable {

    private final Client client;
    private final int modulo;
    private final int threads;
    private final int repeats;
    private final int throughput;
    private final Timer meter;

    public TestRunner(Client client, int modulo, int threads, int repeats, int throughput, Timer meter) {
        this.client = client;
        this.modulo = modulo;
        this.threads = threads;
        this.repeats = repeats;
        this.throughput = throughput;
        this.meter = meter;
    }

    public void run() {
        AtomicInteger done = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(1);

        int perThread = repeats / threads;
        int perMillisecond = throughput / threads / 1000;
        long start = System.nanoTime();
        int pauseTime = 760_000;

        for (int i = modulo, j = 0; i < repeats; i = i + threads, j++) {
            client.run(j * threads + modulo, new OnComplete(done, perThread, latch, meter));

            if (j % perMillisecond == 0) {
                long microsecondsPassed = (System.nanoTime() - start) / 1_000_000;
                if (microsecondsPassed > 0 && j * 1000 / microsecondsPassed > throughput / threads) {
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
        System.out.println(modulo + " done");
    }

    public static class OnComplete implements Runnable {

        private final AtomicInteger done;
        private final int repeats;
        private final CountDownLatch latch;
        private final Timer meter;
        private final long start;

        public OnComplete(AtomicInteger done, int repeats, CountDownLatch latch, Timer meter) {
            this.done = done;
            this.repeats = repeats;
            this.latch = latch;
            this.meter = meter;
            this.start = System.nanoTime();
        }

        @Override
        public void run() {
            int j = done.incrementAndGet();

            meter.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            if (j == repeats) {
                latch.countDown();
            }
        }
    }
}
