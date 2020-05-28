package pl.msulima.redis.benchmark.test.clients;

import pl.msulima.redis.benchmark.jedis.JedisClient;
import pl.msulima.redis.benchmark.test.OnResponse;
import pl.msulima.redis.benchmark.test.TestConfiguration;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

public class AsyncClient implements Client {

    private final JedisClient client;
    private final TestConfiguration configuration;

    public AsyncClient(TestConfiguration configuration) {
        this.configuration = configuration;
        URI redisUri = configuration.getRedisAddresses().get(0);
        this.client = new JedisClient(redisUri.getHost(), configuration.getBatchSize(), configuration.getConcurrency());
    }

    @Override
    public void run(int i, OnResponse onComplete) {
        for (int j = 0; j < configuration.getBatchSize(); j++) {
            int idx = i + j;
            if (configuration.isSet()) {
                client.set(configuration.getKey(idx), configuration.getValue(idx), onComplete::requestFinished);
            } else {
                client.get(configuration.getKey(idx), bytes -> {
                    assert (Arrays.equals(bytes, configuration.getValue(idx)));
                    onComplete.requestFinished();
                });
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
