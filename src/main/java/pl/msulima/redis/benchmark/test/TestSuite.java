package pl.msulima.redis.benchmark.test;

import com.google.common.collect.Lists;
import pl.msulima.redis.benchmark.test.clients.Client;
import pl.msulima.redis.benchmark.test.clients.EmptyClient;
import pl.msulima.redis.benchmark.test.clients.IoClient;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class TestSuite {

    private static final int NUMBER_OF_KEYS = 200_000;
    private static final String KEY_PREFIX = new String(new char[80]).replace("\0", ".");
    private static final String VALUE_PREFIX = new String(new char[80]).replace("\0", ".");
    private static final int SET_RATIO = 20;
    private static final int THROUGHPUT = 50_000;
    private static final int BATCH_SIZE = 2;

    public static void main(String... args) throws InterruptedException {
        byte[][] keys = new byte[NUMBER_OF_KEYS][];
        byte[][] values = new byte[NUMBER_OF_KEYS][];
        for (int i = 0; i < NUMBER_OF_KEYS; i++) {
            keys[i] = (KEY_PREFIX + Integer.toString(i)).getBytes();
            values[i] = (VALUE_PREFIX + i).getBytes();
        }

        String host = System.getProperty("redis.host", "localhost");
        int throughput = Integer.parseInt(System.getProperty("redis.throughput", Integer.toString(THROUGHPUT)));

        TestConfiguration configuration = new TestConfiguration(EmptyClient::new, throughput, host, keys, values, SET_RATIO, BATCH_SIZE, 0, 50);

        syncSuite(configuration);

        System.exit(0);
    }

    private static void syncSuite(TestConfiguration baseConfiguration) {
        List<TestConfiguration> configurations = Lists.newArrayList();

        int throughput = Integer.parseInt(System.getProperty("redis.throughput", Integer.toString(THROUGHPUT)));

        configurations.add(baseConfiguration.copy(IoClient::new, throughput, 1, 4));
//        configurations.add(baseConfiguration.copy(SyncTestClient::new, throughput, 10, 200));

//        for (int i = 15; i > 4; i--) {
//            configurations.add(baseConfiguration.copy(LettuceClient::new, i * 1000, 1, 400));
//            configurations.add(baseConfiguration.copy(SyncTestClient::new, i * 1000, 10, 400));
//            configurations.add(baseConfiguration.copy(AsyncClient::new, i * 1000, 10, 100));
////            configurations.add(baseConfiguration.copy(SyncTestClient::new, i * 1000, 1, 400));
//        }

        runSuite(configurations);
    }

    private static void runSuite(List<TestConfiguration> configurations) {
        configurations.stream().map(TestSuite::runSingle).collect(Collectors.toList());
    }

    private static boolean runSingle(TestConfiguration configuration) {
        Client client = configuration.createClient();

        new TestRunner(client, configuration.getThroughput(), configuration.getThroughput(), configuration.getBatchSize()).run();

        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
}
