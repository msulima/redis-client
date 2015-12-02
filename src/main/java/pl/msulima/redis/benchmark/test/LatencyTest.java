package pl.msulima.redis.benchmark.test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class LatencyTest {

    private final MetricRegistry metrics;

    public LatencyTest(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    public boolean run(int repeats, Client client, int throughput, int batchSize) {
        Timer meter = metrics.timer(String.format(client.name() + " (%d, %d)", throughput, repeats));
        Counter active = metrics.counter(String.format(client.name() + " active (%d, %d)", throughput, repeats));

        TestRunner testRunner = new TestRunner(client, repeats, throughput, batchSize, meter, active);
        return testRunner.run();
    }
}
