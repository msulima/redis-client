package pl.msulima.redis.benchmark.test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

public class TestConfiguration {

    private final Function<TestConfiguration, Client> clientFactory;
    private final int throughput;
    private final String host;
    private final byte[][] keys;
    private final byte[][] values;
    private final int setRatio;
    private final int batchSize;
    private final int pingRatio;
    private final int concurrency;

    public TestConfiguration(Function<TestConfiguration, Client> clientFactory, int throughput, String host, byte[][] keys, byte[][] values,
                             int setRatio, int batchSize, int pingRatio, int concurrency) {
        this.clientFactory = clientFactory;
        this.throughput = throughput;
        this.host = host;
        this.keys = keys;
        this.values = values;
        this.setRatio = setRatio;
        this.batchSize = batchSize;
        this.pingRatio = pingRatio;
        this.concurrency = concurrency;
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

    public boolean isPing() {
        return ThreadLocalRandom.current().nextInt(100) < pingRatio;
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

    public int getThroughput() {
        return throughput;
    }

    public Client createClient() {
        return clientFactory.apply(this);
    }

    public TestConfiguration copy(Function<TestConfiguration, Client> clientFactory, int throughput, int batchSize, int concurrency) {
        return new TestConfiguration(clientFactory, throughput, host, keys, values, setRatio, batchSize, pingRatio, concurrency);
    }

}
