package pl.msulima.redis.benchmark.nonblocking;

import pl.msulima.redis.benchmark.test.OnResponse;
import pl.msulima.redis.benchmark.test.TestConfiguration;
import pl.msulima.redis.benchmark.test.clients.Client;

import java.io.IOException;
import java.util.Arrays;

@SuppressWarnings("Duplicates")
public class NonblockingClient implements Client {

    private final Reactor client;
    private final TestConfiguration configuration;
    private final Thread thread;

    public NonblockingClient(TestConfiguration configuration) {
        this.client = new Reactor(configuration.getPort());
        thread = new Thread(client);
        thread.setName("Client");
        thread.start();
        this.configuration = configuration;
    }

    @Override
    public void run(int i, OnResponse onComplete) {
        for (int j = 0; j < configuration.getBatchSize(); j++) {
            int index = i + j;

            if (configuration.isSet()) {
                client.set(configuration.getKey(index), configuration.getValue(index), onComplete::requestFinished);
            } else {
                client.get(configuration.getKey(index), (bytes) -> {
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
        return "nonblocking";
    }

    @Override
    public void close() throws IOException {
        thread.interrupt();
        try {
            thread.join(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
