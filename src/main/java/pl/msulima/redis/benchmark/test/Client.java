package pl.msulima.redis.benchmark.test;

public interface Client {

    void run(int i, Runnable onComplete);

    String name();
}
