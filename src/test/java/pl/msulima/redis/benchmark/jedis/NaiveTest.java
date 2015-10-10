package pl.msulima.redis.benchmark.jedis;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class NaiveTest {

    public static final int N_THREADS = 4;
    private final MetricRegistry metrics;

    public NaiveTest(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    public void run(int repeats, Client client, int throughput) throws InterruptedException {
        Timer meter = metrics.timer(String.format(client.name() + "(%d)", repeats));

        Thread[] threads = new Thread[N_THREADS];

        for (int i = 0; i < N_THREADS; i++) {
            Thread testRunnerThread = new Thread(new TestRunner(client, i, N_THREADS, repeats, throughput, meter));
            testRunnerThread.start();
            threads[i] = testRunnerThread;
        }

        for (int i = 0; i < N_THREADS; i++) {
            threads[i].join();
        }
    }
}
