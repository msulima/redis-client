package pl.msulima.redis.benchmark.test;

import java.io.IOException;

public class NioClient implements Client {

    private final pl.msulima.redis.benchmark.nio.Client client;
    private final TestConfiguration configuration;

    public NioClient(TestConfiguration configuration) {
        this.configuration = configuration;
        this.client = new pl.msulima.redis.benchmark.nio.Client(configuration.getHost(), 6379);
    }

    public void run(int i, Runnable onComplete) {
        for (int j = 0; j < configuration.getBatchSize(); j++) {
            int index = i + j;

            if (configuration.isPing()) {
                client.ping().thenRun(onComplete);
            } else if (configuration.isSet()) {
                client.set(configuration.getKey(index), configuration.getValue(index)).thenRun(onComplete);
            } else {
                client.get(configuration.getKey(index)).thenAccept(bytes -> {
                    onComplete.run();
                });
            }
        }
    }

    @Override
    public String name() {
        return "nio";
    }

    @Override
    public void close() throws IOException {

    }
}
