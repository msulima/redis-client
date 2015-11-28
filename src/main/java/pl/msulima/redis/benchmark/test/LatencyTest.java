package pl.msulima.redis.benchmark.test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class LatencyTest {

    private final MetricRegistry metrics;

    public LatencyTest(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    public void run(int repeats, Client client, int throughput, int batchSize) throws InterruptedException {
        Timer meter = metrics.timer(String.format(client.name() + "(%d)", repeats));
        Counter active = metrics.counter(String.format(client.name() + " active (%d)", repeats));

        Thread testRunnerThread = new Thread(new TestRunner(client, repeats, throughput, batchSize, meter, active));
        testRunnerThread.start();
        testRunnerThread.join();
    }
}
