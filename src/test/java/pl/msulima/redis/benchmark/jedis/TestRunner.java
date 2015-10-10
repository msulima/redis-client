package pl.msulima.redis.benchmark.jedis;

import com.codahale.metrics.Timer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestRunner implements Runnable {

    private final Client client;
    private final int modulo;
    private final int threads;
    private final int repeats;
    private final Timer meter;

    public TestRunner(Client client, int modulo, int threads, int repeats, Timer meter) {
        this.client = client;
        this.modulo = modulo;
        this.threads = threads;
        this.repeats = repeats;
        this.meter = meter;
    }

    public void run() {
        AtomicInteger done = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(1);

        int perThread = repeats / threads;

        for (int i = modulo; i < repeats; i = i + threads) {
            client.run(i, new OnComplete(done, perThread, latch, meter));
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
