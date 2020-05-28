package pl.msulima.redis.benchmark.test;

import org.apache.commons.cli.*;
import pl.msulima.redis.benchmark.test.clients.Client;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TestSuite {

    private static final int NUMBER_OF_KEYS = Integer.parseInt(System.getProperty("redis.numberOfKeys", "100000"));
    private static final int KEY_PREFIX_SIZE = Integer.parseInt(System.getProperty("redis.keyPrefixSize", "0"));
    private static final int VALUE_SIZE = Integer.parseInt(System.getProperty("redis.valueSize", "100"));
    private static final int SET_RATIO = Integer.parseInt(System.getProperty("redis.setRatio", "20"));

    private static final String KEY_PREFIX = new String(new char[KEY_PREFIX_SIZE]).replace("\0", ".");
    private static final String VALUE_PREFIX = new String(new char[VALUE_SIZE]).replace("\0", ".");

    public static void main(String... args) {
        Options options = new Options();
        options.addOption(Option.builder()
                .longOpt("redis")
                .hasArgs()
                .required(false)
                .build()
        );

        CommandLineParser parser = new DefaultParser();
        CommandLine parse;
        try {
            parse = parser.parse(options, args);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        List<URI> redisAddresses = readRedisAddresses(parse);

        byte[][] keys = new byte[NUMBER_OF_KEYS][];
        byte[][] values = new byte[NUMBER_OF_KEYS][];
        for (int i = 0; i < NUMBER_OF_KEYS; i++) {
            keys[i] = (KEY_PREFIX + i).getBytes();
            values[i] = (VALUE_PREFIX + i).getBytes();
        }

        SuiteProvider.read(keys, values, redisAddresses, SET_RATIO).forEach(TestSuite::runSingle);

        System.exit(0);
    }

    private static List<URI> readRedisAddresses(CommandLine parse) {
        List<String> redis = readRedisOption(parse);
        return redis.stream().map(authority -> URI.create("redis://" + authority)).collect(Collectors.toList());
    }

    private static List<String> readRedisOption(CommandLine parse) {
        String[] redis = parse.getOptionValues("redis");
        if (redis == null) {
            String host = System.getProperty("redis.host", "localhost");
            int port = Integer.parseInt(System.getProperty("redis.port", "6379"));
            return Collections.singletonList(host + ":" + port);
        }
        return Arrays.asList(redis);
    }

    private static void runSingle(TestConfiguration configuration) {
        Client client = configuration.createClient();

        TestRunner testRunner = new TestRunner(client, configuration.getName(),
                configuration.getDuration(), configuration.getThroughput(), configuration.getBatchSize(), configuration.getThreads());
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
