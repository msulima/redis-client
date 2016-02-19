package pl.msulima.redis.benchmark.test.clients;

import pl.msulima.redis.benchmark.test.OnResponse;
import pl.msulima.redis.benchmark.test.TestConfiguration;

import java.io.IOException;

public class IoClient implements Client {

    private final pl.msulima.redis.benchmark.io.IoClient client;
    private final TestConfiguration configuration;

    public IoClient(TestConfiguration configuration) {
        this.configuration = configuration;
        this.client = new pl.msulima.redis.benchmark.io.IoClient(configuration.getHost(), configuration.getPort());
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
                    onComplete.requestFinished();
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
