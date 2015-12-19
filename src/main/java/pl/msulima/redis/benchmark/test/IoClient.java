package pl.msulima.redis.benchmark.test;

import java.io.IOException;

public class IoClient implements Client {

    private final pl.msulima.redis.benchmark.io.IoClient client;
    private final TestConfiguration configuration;

    public IoClient(TestConfiguration configuration) {
        this.configuration = configuration;
        this.client = new pl.msulima.redis.benchmark.io.IoClient(configuration.getHost(), 6379, configuration.getConcurrency());
    }

    public void run(int i, Runnable onComplete) {
        for (int j = 0; j < configuration.getBatchSize(); j++) {
            int index = i + j;

            if (configuration.isPing()) {
                client.ping().thenRun(onComplete);
            } else if (configuration.isSet()) {
                client.set(configuration.getKey(index), configuration.getValue(index), bytes -> {
                    onComplete.run();
                }, System.out::println);
            } else {
                client.get(configuration.getKey(index), bytes -> {
                    onComplete.run();
                }, System.out::println);
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
