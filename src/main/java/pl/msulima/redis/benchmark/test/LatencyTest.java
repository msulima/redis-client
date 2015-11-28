package pl.msulima.redis.benchmark.test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class LatencyTest {

    private static final int N_THREADS = 1;
    private final MetricRegistry metrics;

    public LatencyTest(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    public void run(int repeats, Client client, int throughput) throws InterruptedException {
        Timer meter = metrics.timer(String.format(client.name() + "(%d)", repeats));
        Counter active = metrics.counter(String.format(client.name() + " active (%d)", repeats));

        Thread[] threads = new Thread[N_THREADS];

        for (int i = 0; i < N_THREADS; i++) {
            Thread testRunnerThread = new Thread(new TestRunner(client, i, N_THREADS, repeats, throughput, meter, active));
            testRunnerThread.start();
            threads[i] = testRunnerThread;
        }

        for (int i = 0; i < N_THREADS; i++) {
            threads[i].join();
        }
    }
}
