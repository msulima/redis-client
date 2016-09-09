package pl.msulima.redis.benchmark.test;

import pl.msulima.redis.benchmark.test.clients.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class SuiteProvider {

    public static List<TestConfiguration> read(byte[][] keys, byte[][] values,
                                               int setRatio) {
        String host = System.getProperty("redis.host", "localhost");
        int port = Integer.parseInt(System.getProperty("redis.port", "6379"));

        try {
            List<String> lines = Files.readAllLines(new File(System.getProperty("redis.schedule", "schedule.csv")).toPath());
            List<TestConfiguration> tests = new ArrayList<>(lines.size());

            for (String line : lines) {
                if (line.length() == 0 || line.startsWith("#")) {
                    continue;
                }

                String[] strings = line.split("\\s+");
                Function<TestConfiguration, Client> factory = clientFactory(strings[0]);
                int duration = Integer.parseInt(strings[1]);
                int throughput = Integer.parseInt(strings[2]);
                int batchSize = Integer.parseInt(strings[3]);
                int concurrency = Integer.parseInt(strings[4]);
                boolean closeable = Boolean.parseBoolean(strings[5]);

                String[] name = new String[strings.length - 6 + 1];
                name[0] = strings[0] + ":";
                System.arraycopy(strings, 6, name, 1, strings.length - 6);

                String nameString = String.join(" ", name);

                tests.add(new TestConfiguration(factory, duration, throughput, host, port, keys, values, setRatio, batchSize, concurrency, closeable, nameString));
            }

            return tests;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Function<TestConfiguration, Client> clientFactory(String name) {
        switch (name) {
            case "io":
                return IoClient::new;
            case "sync":
                return SyncPooledClient::new;
            case "sync_tl":
                return SyncThreadLocalClient::new;
            case "async":
                return AsyncClient::new;
            case "empty":
                return EmptyClient::new;
            case "lettuce":
                return LettuceClient::new;
            default:
                throw new IllegalArgumentException("Unknown client " + name);
        }
    }
}
