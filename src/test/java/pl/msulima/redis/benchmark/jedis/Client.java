package pl.msulima.redis.benchmark.jedis;

public interface Client {

    void run(int i, Runnable onComplete);

    String name();
}
