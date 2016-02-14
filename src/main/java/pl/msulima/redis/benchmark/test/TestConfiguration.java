package pl.msulima.redis.benchmark.test;

import pl.msulima.redis.benchmark.test.clients.Client;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

public class TestConfiguration {

    private final Function<TestConfiguration, Client> clientFactory;
    private final int duration;
    private final int throughput;
    private final String host;
    private final int port;
    private final byte[][] keys;
    private final byte[][] values;
    private final int setRatio;
    private final int batchSize;
    private final int concurrency;
    private final boolean closeable;
    private final String name;

    public TestConfiguration(Function<TestConfiguration, Client> clientFactory, int duration, int throughput, String host, int port, byte[][] keys, byte[][] values,
                             int setRatio, int batchSize, int concurrency, boolean closeable, String name) {
        this.clientFactory = clientFactory;
        this.duration = duration;
        this.throughput = throughput;
        this.host = host;
        this.port = port;
        this.keys = keys;
        this.values = values;
        this.setRatio = setRatio;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.closeable = closeable;
        this.name = name;
    }

    public byte[] getKey(int i) {
        return keys[(i % keys.length)];
    }

    public byte[] getValue(int i) {
        return values[(i % values.length)];
    }

    public boolean isSet() {
        return ThreadLocalRandom.current().nextInt(100) < setRatio;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getThroughput() {
        return throughput;
    }

    public Client createClient() {
        return clientFactory.apply(this);
    }

    public int getDuration() {
        return duration;
    }

    public String getName() {
        return name;
    }

    public boolean isCloseable() {
        return closeable;
    }
}
