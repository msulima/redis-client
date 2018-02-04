package pl.msulima.redis.benchmark.test.clients;

import pl.msulima.redis.benchmark.io.SyncClient;
import pl.msulima.redis.benchmark.test.OnResponse;
import pl.msulima.redis.benchmark.test.TestConfiguration;

import java.io.IOException;
import java.util.Arrays;

public class IoClient implements Client {

    private final SyncClient client;
    private final TestConfiguration configuration;

    public IoClient(TestConfiguration configuration) {
        this.configuration = configuration;
        this.client = new SyncClient(configuration.getHost(), configuration.getPort());
    }

    public void run(int i, OnResponse onComplete) {
        for (int j = 0; j < configuration.getBatchSize(); j++) {
            int index = i + j;

            if (configuration.isSet()) {
                client.set(configuration.getKey(index), configuration.getValue(index), (bytes, error) -> {
                    if (error != null) {
                        System.err.println(error.getMessage());
                    }
                    onComplete.requestFinished();
                });
            } else {
                client.get(configuration.getKey(index), (bytes, error) -> {
                    if (bytes == null || Arrays.equals(bytes, configuration.getValue(index))) {
                        onComplete.requestFinished();
                    } else {
                        throw new RuntimeException("Wrong result for key" + new String(configuration.getKey(index)));
                    }
                });
            }
        }
    }

    @Override
    public String name() {
        return "io";
    }

    @Override
    public void close() throws IOException {

    }
}
