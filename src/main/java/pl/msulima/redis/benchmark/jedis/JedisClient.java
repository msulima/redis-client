package pl.msulima.redis.benchmark.jedis;

import redis.clients.jedis.JedisPool;
import uk.co.real_logic.agrona.concurrent.ManyToOneConcurrentArrayQueue;

import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

public class JedisClient {

    private final static int SLEEP_TIME = 1;
    private final int size;
    private final ManyToOneConcurrentArrayQueue<Operation> requests;
    private final Executor singlePool = Executors.newSingleThreadExecutor();
    private final Executor pool = Executors.newFixedThreadPool(80);
    private final JedisPool jedisPool;

    public JedisClient(String host, int capacity) {
        this.size = capacity;
        this.jedisPool = new JedisPool(host);
        requests = new ManyToOneConcurrentArrayQueue<>(capacity);
        Timer timer = new Timer(false);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                drain();
            }
        }, SLEEP_TIME, SLEEP_TIME);
    }

    public void get(byte[] key, Function<byte[], Void> callback) {
        addRequest(new GetOperation(key, callback));
    }

    public void set(byte[] key, byte[] value, Runnable callback) {
        addRequest(new SetOperation(key, value, callback));
    }

    public void addRequest(Operation request) {
        if (!requests.offer(request)) {
            drain();

            while (!requests.offer(request)) {
                LockSupport.parkNanos(1);
            }
        }
    }

    private void drain() {
        singlePool.execute(() -> {
            ArrayList<Operation> objects = new ArrayList<>(size);
            requests.drainTo(objects, size);
            if (!objects.isEmpty()) {
                pool.execute(new JedisClientWorker(jedisPool, objects));
            }
        });
    }
}
