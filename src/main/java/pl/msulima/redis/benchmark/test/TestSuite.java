package pl.msulima.redis.benchmark.test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestSuite {

    private static final int NUMBER_OF_KEYS = 200_000;
    private static final String KEY_PREFIX = new String(new char[80]).replace("\0", ".");
    private static final String VALUE_PREFIX = new String(new char[8000]).replace("\0", ".");
    private static final int SET_RATIO = 5;
    private static final int THROUGHPUT = 50_000;

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

        String host = "localhost";
        if (args.length > 0) {
            host = args[0];
        }

        Client syncClient = new SyncTestClient(host, keys, values, SET_RATIO);
        Client emptyClient = new EmptyClient();
        LatencyTest latencyTest = new LatencyTest(metrics);
        Client asyncClient = new AsyncClient(host, keys, values, 100, SET_RATIO);
        Client lettuce = new LettuceClient(host, keys, values, SET_RATIO);

        List<Client> clients = Lists.newArrayList(lettuce, syncClient, asyncClient);

        int throughput = THROUGHPUT;
        if (args.length > 1) {
            throughput = Integer.parseInt(args[1]);
        }

        if (args.length > 2) {
            String clientName = args[2];

            for (Client client : clients) {
                if (client.name().equals(clientName)) {
                    latencyTest.run(throughput * 10, client, throughput / 2);
                    latencyTest.run(throughput * 60, client, throughput);
                }
            }
        } else {
            for (Client client : clients) {
                latencyTest.run(throughput * 10, client, throughput / 2);
                latencyTest.run(throughput * 60, client, throughput);
            }
        }

        System.exit(0);
    }
}
