package pl.msulima.redis.benchmark.test.clients;

import pl.msulima.redis.benchmark.jedis.JedisClient;
import pl.msulima.redis.benchmark.test.TestConfiguration;

import java.io.IOException;

class AsyncClient implements Client {

    private final JedisClient client;
    private final TestConfiguration configuration;

    public AsyncClient(TestConfiguration configuration) {
        this.configuration = configuration;
        this.client = new JedisClient(configuration.getHost(), configuration.getBatchSize(), configuration.getConcurrency());
    }

    public void run(int i, Runnable onComplete) {
        for (int j = 0; j < configuration.getBatchSize(); j++) {
            if (configuration.isSet()) {
                client.set(configuration.getKey(i + j), configuration.getValue(i + j), onComplete::run);
            } else {
                client.get(configuration.getKey(i + j), bytes -> onComplete.run());
            }
        }
    }

    @Override
    public String name() {
        return String.format("async %d %d", configuration.getBatchSize(), configuration.getConcurrency());
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
