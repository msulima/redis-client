package pl.msulima.redis.benchmark.test;

import java.io.IOException;

public class IoClient implements Client {

    private final pl.msulima.redis.benchmark.io.IoClient client;
    private final TestConfiguration configuration;

    public IoClient(TestConfiguration configuration) {
        this.configuration = configuration;
        this.client = new pl.msulima.redis.benchmark.io.IoClient();
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
        return "io";
    }

    @Override
    public void close() throws IOException {

    }
}
