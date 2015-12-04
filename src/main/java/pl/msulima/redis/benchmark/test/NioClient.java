package pl.msulima.redis.benchmark.test;

import java.io.IOException;
import java.util.Arrays;

public class NioClient implements Client {

    private final pl.msulima.redis.benchmark.nio.Client client;
    private final TestConfiguration configuration;

    public NioClient(TestConfiguration configuration) {
        this.configuration = configuration;
        this.client = new pl.msulima.redis.benchmark.nio.Client();
    }

    public void run(int i, Runnable onComplete) {
        for (int j = 0; j < configuration.getBatchSize(); j++) {
            int index = i + j;
            byte[] value = ("nio" + new String(configuration.getValue(index))).getBytes();
            if (configuration.isPing()) {
                client.ping().thenRun(onComplete);
            } else if (configuration.isSet()) {
                client.set(configuration.getKey(index), value).thenRun(onComplete);
            } else {
                client.get(configuration.getKey(index)).thenAccept(bytes -> {
                    if (bytes != null) {
                        assert Arrays.equals(bytes, configuration.getValue(index)) || Arrays.equals(bytes, value);
                    }
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
