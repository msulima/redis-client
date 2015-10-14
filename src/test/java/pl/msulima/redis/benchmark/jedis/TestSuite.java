package pl.msulima.redis.benchmark.jedis;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

public class TestSuite {

    private static final int NUMBER_OF_KEYS = 1_000_000;
    private static final int WARMUP = 1_000_000;
    private static final int REPEATS = 5_000_000;
    private static final String VALUE_PREFIX = "....................................................................................................";
    private static final int SET_RATIO = 5;
    private static final int THROUGHPUT = 10_000;

    public static void main(String... args) throws InterruptedException {
        MetricRegistry metrics = new MetricRegistry();
        ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build().start(3, TimeUnit.SECONDS);

        byte[][] keys = new byte[NUMBER_OF_KEYS][];
        byte[][] values = new byte[NUMBER_OF_KEYS][];
        for (int i = 0; i < NUMBER_OF_KEYS; i++) {
            keys[i] = Integer.toString(i).getBytes();
            values[i] = (VALUE_PREFIX + i).getBytes();
        }

        Client syncClient = new SyncTestClient(keys, values, SET_RATIO);
        Client emptyClient = new EmptyClient();
        LatencyTest latencyTest = new LatencyTest(metrics);
        AsyncClient asyncClient = new AsyncClient(keys, values, 100, SET_RATIO);
        Client client = syncClient;
        latencyTest.run(WARMUP, client, THROUGHPUT);
        latencyTest.run(REPEATS, client, THROUGHPUT);

        System.exit(0);
    }
}
