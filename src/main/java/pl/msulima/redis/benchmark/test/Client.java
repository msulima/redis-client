package pl.msulima.redis.benchmark.test;

import java.io.Closeable;

public interface Client extends Closeable {

    void run(int i, Runnable onComplete);

    String name();
}
