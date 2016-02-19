package pl.msulima.redis.benchmark.test;

import pl.msulima.redis.benchmark.test.clients.Client;

import java.io.IOException;

public class TestSuite {

    private static final int NUMBER_OF_KEYS = 200_000;
    public static final int KEY_SIZE = Integer.parseInt(System.getProperty("redis.keySize", "80"));
    public static final int VALUE_SIZE = Integer.parseInt(System.getProperty("redis.valueSize", "80"));

    private static final String KEY_PREFIX = new String(new char[KEY_SIZE]).replace("\0", ".");
    private static final String VALUE_PREFIX = new String(new char[VALUE_SIZE]).replace("\0", ".");
    private static final int SET_RATIO = 20;

    public static void main(String... args) throws InterruptedException {
        byte[][] keys = new byte[NUMBER_OF_KEYS][];
        byte[][] values = new byte[NUMBER_OF_KEYS][];
        for (int i = 0; i < NUMBER_OF_KEYS; i++) {
            keys[i] = (KEY_PREFIX + Integer.toString(i)).getBytes();
            values[i] = (VALUE_PREFIX + i).getBytes();
        }

        SuiteProvider.read(keys, values, SET_RATIO).forEach(TestSuite::runSingle);

        System.exit(0);
    }

    private static void runSingle(TestConfiguration configuration) {
        Client client = configuration.createClient();

        TestRunner testRunner = new TestRunner(client, configuration.getName(),
                configuration.getDuration(), configuration.getThroughput(), configuration.getBatchSize());
        testRunner.run();

        if (configuration.isCloseable()) {
            try {
                client.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
