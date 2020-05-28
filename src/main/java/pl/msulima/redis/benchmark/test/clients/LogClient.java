package pl.msulima.redis.benchmark.test.clients;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.msulima.redis.benchmark.log.ClientFacade;
import pl.msulima.redis.benchmark.log.Driver;
import pl.msulima.redis.benchmark.log.protocol.DynamicEncoder;
import pl.msulima.redis.benchmark.log.transport.SocketChannelTransportFactory;
import pl.msulima.redis.benchmark.log.transport.TransportFactory;
import pl.msulima.redis.benchmark.test.OnResponse;
import pl.msulima.redis.benchmark.test.TestConfiguration;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class LogClient implements Client {

    private static final Logger log = LoggerFactory.getLogger(LogClient.class);


    private final Driver driver;
    private final TestConfiguration configuration;
    private final ClientFacade[] connections;

    public LogClient(TestConfiguration configuration) {
        this.configuration = configuration;
        TransportFactory transportFactory = new SocketChannelTransportFactory();
        this.driver = new Driver(transportFactory, 0);
        this.driver.start();
        List<URI> redisAddresses = configuration.getRedisAddresses();
        this.connections = new ClientFacade[redisAddresses.size()];
        for (int i = 0; i < redisAddresses.size(); i++) {
            URI redisUri = redisAddresses.get(i);
            try {
                connections[i] = driver.connect(new InetSocketAddress(redisUri.getHost(), redisUri.getPort())).toCompletableFuture().get(10, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void run(int i, OnResponse onComplete) {
        for (int j = 0; j < configuration.getBatchSize(); j++) {
            int idx = i + j;
            byte[] key = configuration.getKey(idx);
            byte[] value = configuration.getValue(idx);
            ClientFacade connection = connections[idx % connections.length];
            if (configuration.isSet()) {
                connection.set(key, value).thenRun(onComplete::requestFinished);
            } else {
                connection.get(key).thenAccept(bytes -> {
                    if (!Arrays.equals(bytes, value)) {
                        log.error("Received unexpected value for key: {}, expected: {}, actual: {}",
                                new String(key, DynamicEncoder.CHARSET), new String(value, DynamicEncoder.CHARSET), new String(bytes, DynamicEncoder.CHARSET));
                        throw new RuntimeException("Unexpected value");
                    }
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
