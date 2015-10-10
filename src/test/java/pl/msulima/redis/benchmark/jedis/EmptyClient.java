package pl.msulima.redis.benchmark.jedis;

public class EmptyClient implements Client {

    @Override
    public void run(int i, Runnable onComplete) {
        onComplete.run();
    }
}
