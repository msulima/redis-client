package pl.msulima.redis.benchmark.jedis;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class NaiveTest {

    public static final int N_THREADS = 100;
    private final int setRatio;
    private final byte[][] keys;
    private final byte[][] values;
    private final MetricRegistry metrics;

    public NaiveTest(int setRatio, byte[][] keys, byte[][] values, MetricRegistry metrics) {
        this.setRatio = setRatio;
        this.keys = keys;
        this.values = values;
        this.metrics = metrics;
    }

    public void run(int repeats) throws InterruptedException {
        Timer meter = metrics.timer(String.format("ThroughputTest(%d)", repeats));
        Client client = new SyncTestClient(keys, values, setRatio);
//        Client client = new EmptyClient();

        Thread[] threads = new Thread[N_THREADS];

        for (int i = 0; i < N_THREADS; i++) {
            Thread testRunnerThread = new Thread(new TestRunner(client, i, N_THREADS, repeats, meter));
            testRunnerThread.start();
            threads[i] = testRunnerThread;
        }

        for (int i = 0; i < N_THREADS; i++) {
            threads[i].join();
        }
    }
}
