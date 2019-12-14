package pl.msulima.redis.benchmark.test.clients;

import pl.msulima.redis.benchmark.log.ClientFacade;
import pl.msulima.redis.benchmark.log.Driver;
import pl.msulima.redis.benchmark.log.transport.SocketChannelTransportFactory;
import pl.msulima.redis.benchmark.log.transport.TransportFactory;
import pl.msulima.redis.benchmark.test.OnResponse;
import pl.msulima.redis.benchmark.test.TestConfiguration;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class LogClient implements Client {

    private final Driver driver;
    private final TestConfiguration configuration;
    private final ClientFacade connection;

    public LogClient(TestConfiguration configuration) {
        this.configuration = configuration;
        TransportFactory transportFactory = new SocketChannelTransportFactory();
        this.driver = new Driver(transportFactory, 0);
        driver.start();
        try {
            this.connection = driver.connect(new InetSocketAddress(configuration.getHost(), configuration.getPort())).toCompletableFuture().get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run(int i, OnResponse onComplete) {
        for (int j = 0; j < configuration.getBatchSize(); j++) {
            int idx = i + j;
            if (configuration.isSet()) {
                connection.set(configuration.getKey(idx), configuration.getValue(idx)).thenRun(onComplete::requestFinished);
            } else {
                connection.get(configuration.getKey(idx)).thenAccept(bytes -> {
                    assert (Arrays.equals(bytes, configuration.getValue(idx)));
                    onComplete.requestFinished();
                });
            }
        }
    }

    @Override
    public String name() {
        return String.format("log %d %d", configuration.getBatchSize(), configuration.getConcurrency());
    }

    @Override
    public void close() {
        driver.close();
    }
}
