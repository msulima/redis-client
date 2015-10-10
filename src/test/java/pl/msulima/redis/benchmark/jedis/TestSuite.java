package pl.msulima.redis.benchmark.jedis;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

public class TestSuite {

    private static final int NUMBER_OF_KEYS = 1_000_000;
    private static final int REPEATS = 5_000_000;
    private static final String VALUE_PREFIX = "....................................................................................................";

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

        NaiveTest naiveTest = new NaiveTest(5, keys, values, metrics);
        naiveTest.run(REPEATS);
//        ThroughputTest test = new ThroughputTest(5, keys, values, metrics);
//        test.run(100, REPEATS);

        System.exit(0);
    }
}
