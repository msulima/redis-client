package pl.msulima.queues.benchmark;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public class Metrics {

    private final MetricRegistry metrics;
    private final String name;
    private final TimerTask task;

    public final Timer latencyTimer;
    public final Timer serviceTimer;
    public final Timer responseTimer;
    public final AtomicInteger inSystem;

    public Metrics(MetricRegistry metrics, String name, java.util.Timer timer) {
        this.metrics = metrics;
        this.name = name;
        this.latencyTimer = metrics.timer("latency" + name);
        this.serviceTimer = metrics.timer("service" + name);
        metrics.remove("service" + name);
        this.responseTimer = metrics.timer("response" + name);
        inSystem = new AtomicInteger();
        task = new TimerTask() {
            @Override
            public void run() {
                metrics.histogram("inSystem" + name).update(inSystem.get());
            }
        };
        timer.scheduleAtFixedRate(task, 0, 100);
    }

    public void shutdown() {
        task.cancel();
        metrics.remove("latency" + name);
        metrics.remove("service" + name);
        metrics.remove("response" + name);
        metrics.remove("inSystem" + name);
    }
}
