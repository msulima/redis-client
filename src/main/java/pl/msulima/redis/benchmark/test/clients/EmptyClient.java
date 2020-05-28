package pl.msulima.redis.benchmark.test.clients;

import pl.msulima.redis.benchmark.test.OnResponse;
import pl.msulima.redis.benchmark.test.TestConfiguration;

public class EmptyClient implements Client {

    private final TestConfiguration configuration;

    public EmptyClient(TestConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void run(int i, OnResponse onComplete) {
        for (int j = 0; j < configuration.getBatchSize(); j++) {
            onComplete.requestFinished();
        }
    }

    @Override
    public String name() {
        return "empty";
    }

    @Override
    public void close() {
    }
}
