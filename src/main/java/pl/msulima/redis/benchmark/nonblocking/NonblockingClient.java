package pl.msulima.redis.benchmark.nonblocking;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import pl.msulima.redis.benchmark.test.OnResponse;
import pl.msulima.redis.benchmark.test.TestConfiguration;
import pl.msulima.redis.benchmark.test.clients.Client;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ThreadFactory;

@SuppressWarnings("Duplicates")
public class NonblockingClient implements Client {

    private final Reactor clients[];
    private final TestConfiguration configuration;
    private final Thread threads[];
    private int idx = 0;

    public NonblockingClient(TestConfiguration configuration) {
        int concurrency = configuration.getConcurrency();

        this.clients = new Reactor[concurrency];
        this.threads = new Thread[concurrency];
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("Reactor-%d").build();

        for (int i = 0; i < clients.length; i++) {
            clients[i] = new Reactor(configuration.getPort() + i);
            threads[i] = threadFactory.newThread(clients[i]);
            threads[i].start();
        }

        this.configuration = configuration;
    }

    @Override
    public void run(int i, OnResponse onComplete) {
        for (int j = 0; j < configuration.getBatchSize(); j++) {
            int index = i + j;
            Reactor client = clients[idx++ % clients.length];

            if (configuration.isSet()) {
                client.set(configuration.getKey(index), configuration.getValue(index), onComplete::requestFinished);
            } else {
                client.get(configuration.getKey(index), (bytes) -> {
                    if (bytes == null || Arrays.equals(bytes, configuration.getValue(index))) {
                        onComplete.requestFinished();
                    } else {
                        throw new RuntimeException("Wrong result for key" + new String(configuration.getKey(index)) + " " + new String(bytes));
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
        for (Thread thread : threads) {
            thread.interrupt();
        }

        for (Thread thread : threads) {
            try {
                thread.join(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
