package pl.msulima.redis.benchmark.jedis;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ThroughputTest {

    private final int setRatio;
    private final byte[][] keys;
    private final byte[][] values;
    private final MetricRegistry metrics;

    public ThroughputTest(int setRatio, byte[][] keys, byte[][] values, MetricRegistry metrics) {
        this.setRatio = setRatio;
        this.keys = keys;
        this.values = values;
        this.metrics = metrics;
    }

    public void run(int batchSize, int repeats) throws InterruptedException {
        Timer meter = metrics.timer(String.format("ThroughputTest(%d, %d)", batchSize, repeats));
        ThroughputTestClient client = new ThroughputTestClient(keys, values, batchSize, meter, setRatio);

        AtomicInteger done = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(1);

        int throughput = 120_000;
        int sleepTime = 25;
        int perBatch = throughput / (1000 / sleepTime);

        for (int i = 0; i < Math.ceil((double) repeats / perBatch); i++) {
            long start = System.currentTimeMillis();
            for (int j = 0; j < perBatch; j++) {
                client.run(i * perBatch + j, new OnComplete(done, repeats, latch, client));
            }

            long millis = start + sleepTime - System.currentTimeMillis();
            if (millis > 0) {
                Thread.sleep(millis);
            }
        }

        latch.await();
    }

    public static class OnComplete implements Runnable {

        private final AtomicInteger done;
        private final int repeats;
        private final CountDownLatch latch;

        public OnComplete(AtomicInteger done, int repeats, CountDownLatch latch, ThroughputTestClient client) {
            this.done = done;
            this.repeats = repeats;
            this.latch = latch;
        }

        @Override
        public void run() {
            int j = done.incrementAndGet();

            if (j == repeats) {
                latch.countDown();
            }
        }
    }

    public static class ThroughputTestClient {

        private final Timer meter;
        private final int setRatio;
        private final byte[][] keys;
        private final byte[][] values;
        private final JedisClient client;

        public ThroughputTestClient(byte[][] keys, byte[][] values, int batchSize, Timer meter, int setRatio) {
            this.keys = keys;
            this.values = values;
            this.meter = meter;
            this.setRatio = setRatio;
            this.client = new JedisClient(batchSize);
        }

        public void run(int i, Runnable onComplete) {
            long start = System.currentTimeMillis();

            int k = i % keys.length;

            if (i % setRatio == 0) {
                client.set(keys[k], values[k], () -> {
                    meter.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                    onComplete.run();
                });
            } else {
                client.get(keys[k], bytes -> {
                    meter.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                    onComplete.run();
                    return null;
                });
            }
        }
    }
}
