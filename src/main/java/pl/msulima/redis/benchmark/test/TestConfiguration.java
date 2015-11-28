package pl.msulima.redis.benchmark.test;

import java.util.concurrent.ThreadLocalRandom;

public class TestConfiguration {

    private final String host;
    private final byte[][] keys;
    private final byte[][] values;
    private final int setRatio;
    private final int batchSize;
    private final int pingRatio;
    private final int concurrency;

    public TestConfiguration(String host, byte[][] keys, byte[][] values,
                             int setRatio, int batchSize, int pingRatio, int concurrency) {
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
        return ThreadLocalRandom.current().nextInt() % 100 < setRatio;
    }

    public boolean isPing() {
        return ThreadLocalRandom.current().nextInt() % 100 < pingRatio;
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
}
