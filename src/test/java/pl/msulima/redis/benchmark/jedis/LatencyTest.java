package pl.msulima.redis.benchmark.jedis;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class LatencyTest {

    private final int setRatio;
    private final byte[][] keys;
    private final byte[][] values;
    private final MetricRegistry metrics;

    public LatencyTest(int setRatio, byte[][] keys, byte[][] values, MetricRegistry metrics) {
        this.setRatio = setRatio;
        this.keys = keys;
        this.values = values;
        this.metrics = metrics;
    }

    public void run(int batchSize, int repeats) throws InterruptedException {
        ThroughputTestClient client = new ThroughputTestClient(keys, values, batchSize, setRatio);
        Timer meter = metrics.timer(String.format("ThroughputTest(%d, %d)", batchSize, repeats));

        AtomicInteger done = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(1);

        for (int i = 0; i < repeats; i++) {
            long start = System.currentTimeMillis();
            client.run(i, () -> {
                int j = done.incrementAndGet();
                meter.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                if (j == repeats) {
                    latch.countDown();
                }
            });
        }

        latch.await();
    }

    public static class ThroughputTestClient {

        private final int setRatio;
        private final byte[][] keys;
        private final byte[][] values;
        private final JedisClient client;

        public ThroughputTestClient(byte[][] keys, byte[][] values, int batchSize, int setRatio) {
            this.setRatio = setRatio;
            this.keys = keys;
            this.values = values;
            this.client = new JedisClient(batchSize);
        }

        public void run(int i, Runnable onComplete) {
            Runnable setCallback = onComplete::run;
            Function<byte[], Void> getCallback = bytes -> {
                onComplete.run();
                return null;
            };

            int k = i % keys.length;

            if (i % setRatio == 0) {
                client.set(keys[k], values[k], setCallback);
            } else {
                client.get(keys[k], getCallback);
            }
        }
    }
}
