package pl.msulima.redis.benchmark.jedis;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import uk.co.real_logic.agrona.concurrent.ManyToOneConcurrentArrayQueue;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

public class JedisClient implements Closeable {

    private final static int SLEEP_TIME = 1;
    private final int size;
    private final ManyToOneConcurrentArrayQueue<Operation> requests;
    private final ExecutorService singlePool = Executors.newSingleThreadExecutor();
    private final ExecutorService pool;
    private final JedisPool jedisPool;
    private final Timer timer;

    public JedisClient(String host, int capacity, int concurrency) {
        this.size = capacity;
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(concurrency);
        poolConfig.setMaxTotal(concurrency);
        this.jedisPool = new JedisPool(host);
        requests = new ManyToOneConcurrentArrayQueue<>(capacity);
        timer = new Timer(false);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                drain();
            }
        }, SLEEP_TIME, SLEEP_TIME);
        this.pool = Executors.newFixedThreadPool(concurrency);
    }

    public void get(byte[] key, Consumer<byte[]> callback) {
        addRequest(new GetOperation(key, callback));
    }

    public void set(byte[] key, byte[] value, Runnable callback) {
        addRequest(new SetOperation(key, value, callback));
    }

    private void addRequest(Operation request) {
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

    @Override
    public void close() throws IOException {
        timer.cancel();
        try {
            pool.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        singlePool.shutdown();
        pool.shutdown();
        jedisPool.close();
    }
}
