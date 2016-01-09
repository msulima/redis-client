package pl.msulima.redis.benchmark.test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TestSuite {

    private static final int NUMBER_OF_KEYS = 200_000;
    private static final String KEY_PREFIX = new String(new char[80]).replace("\0", ".");
    private static final String VALUE_PREFIX = new String(new char[80]).replace("\0", ".");
    private static final int SET_RATIO = 20;
    private static final int THROUGHPUT = 300_000;
    private static final int BATCH_SIZE = 2;

    public static void main(String... args) throws InterruptedException {
        MetricRegistry metrics = new MetricRegistry();
        ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build().start(3, TimeUnit.SECONDS);

        byte[][] keys = new byte[NUMBER_OF_KEYS][];
        byte[][] values = new byte[NUMBER_OF_KEYS][];
        for (int i = 0; i < NUMBER_OF_KEYS; i++) {
            keys[i] = (KEY_PREFIX + Integer.toString(i)).getBytes();
            values[i] = (VALUE_PREFIX + i).getBytes();
        }

        String host = System.getProperty("redis.host", "localhost");
        int throughput = Integer.parseInt(System.getProperty("redis.throughput", Integer.toString(THROUGHPUT)));

        LatencyTest latencyTest = new LatencyTest(metrics);

        TestConfiguration configuration = new TestConfiguration(EmptyClient::new, throughput, host, keys, values, SET_RATIO, BATCH_SIZE, 0, 50);

        syncSuite(latencyTest, configuration);

        System.exit(0);
    }

    private static void syncSuite(LatencyTest latencyTest, TestConfiguration baseConfiguration) {
        List<TestConfiguration> configurations = Lists.newArrayList();

        int throughput = Integer.parseInt(System.getProperty("redis.throughput", Integer.toString(THROUGHPUT)));

        configurations.add(baseConfiguration.copy(IoClient::new, throughput, 1, 4));
//        configurations.add(baseConfiguration.copy(NioClient::new, throughput, 1, 4));
//        configurations.add(baseConfiguration.copy(SyncTestClient::new, throughput, 10, 200));

//        for (int i = 15; i > 4; i--) {
//            configurations.add(baseConfiguration.copy(LettuceClient::new, i * 1000, 1, 400));
//            configurations.add(baseConfiguration.copy(SyncTestClient::new, i * 1000, 10, 400));
//            configurations.add(baseConfiguration.copy(AsyncClient::new, i * 1000, 10, 100));
////            configurations.add(baseConfiguration.copy(SyncTestClient::new, i * 1000, 1, 400));
//        }

        runSuite(latencyTest, configurations);
    }

    private static void runSuite(LatencyTest latencyTest, List<TestConfiguration> configurations) {
        configurations.stream().map(configuration -> runSingle(latencyTest, configuration)).collect(Collectors.toList());
    }

    private static boolean runSingle(LatencyTest latencyTest, TestConfiguration configuration) {
        Client client = configuration.createClient();

        latencyTest.run(configuration.getThroughput() * 300, client, configuration.getThroughput(), configuration.getBatchSize());

        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
}
