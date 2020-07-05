package pl.msulima.redis.benchmark.test;

import pl.msulima.redis.benchmark.test.clients.Client;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

public class TestConfiguration {

    final Function<TestConfiguration, Client> clientFactory;
    final int duration;
    final int throughput;
    final List<URI> redisAddresses;
    final byte[][] keys;
    final byte[][] values;
    final int setRatio;
    final int batchSize;
    final int concurrency;
    final boolean closeable;
    final String name;
    final int threads;

    public TestConfiguration(Function<TestConfiguration, Client> clientFactory, int duration, int throughput, List<URI> redisAddresses, byte[][] keys, byte[][] values,
                             int setRatio, int batchSize, int concurrency, boolean closeable, int threads, String name) {
        this.clientFactory = clientFactory;
        this.duration = duration;
        this.throughput = throughput;
        this.redisAddresses = redisAddresses;
        this.keys = keys;
        this.values = values;
        this.setRatio = setRatio;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.closeable = closeable;
        this.threads = threads;
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

    public List<URI> getRedisAddresses() {
        return redisAddresses;
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

    public int getThreads() {
        return threads;
    }
}
