package pl.msulima.redis.benchmark.test;

public class EmptyClient implements Client {

    @Override
    public void run(int i, Runnable onComplete) {
        onComplete.run();
    }

    @Override
    public String name() {
        return "empty";
    }
}
