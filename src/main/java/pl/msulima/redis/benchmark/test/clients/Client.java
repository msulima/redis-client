package pl.msulima.redis.benchmark.test.clients;

import pl.msulima.redis.benchmark.test.OnResponse;

import java.io.Closeable;

public interface Client extends Closeable {

    void run(int i, OnResponse onResponse);

    String name();
}
