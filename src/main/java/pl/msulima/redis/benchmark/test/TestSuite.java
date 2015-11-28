package pl.msulima.redis.benchmark.test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestSuite {

    private static final int NUMBER_OF_KEYS = 200_000;
    private static final String KEY_PREFIX = new String(new char[80]).replace("\0", ".");
    private static final String VALUE_PREFIX = new String(new char[80]).replace("\0", ".");
    private static final int SET_RATIO = 5;
    private static final int THROUGHPUT = 25_000;
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

        TestConfiguration configuration = new TestConfiguration(host, keys, values, SET_RATIO, BATCH_SIZE, 0, 50);
        Client syncClient = new SyncTestClient(configuration);
        Client emptyClient = new EmptyClient(configuration);
        LatencyTest latencyTest = new LatencyTest(metrics);
        Client lettuce = new LettuceClient(configuration);

        List<Client> clients = Lists.newArrayList(lettuce, syncClient, emptyClient);

        int throughput = Integer.parseInt(System.getProperty("redis.throughput", Integer.toString(THROUGHPUT)));

        if (args.length > 0) {
            String clientName = args[0];

            for (Client client : clients) {
                if (client.name().equals(clientName)) {
                    latencyTest.run(throughput * 10, client, throughput / 2, configuration.getBatchSize());
                    latencyTest.run(throughput * 60, client, throughput, configuration.getBatchSize());
                }
            }
        } else {
            for (Client client : clients) {
                latencyTest.run(throughput * 10, client, throughput / 2, configuration.getBatchSize());
                latencyTest.run(throughput * 60, client, throughput, configuration.getBatchSize());
            }
        }

        System.exit(0);
    }
}
