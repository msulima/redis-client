package pl.msulima.redis.benchmark.io.connection;

import redis.clients.jedis.Protocol;

import java.util.function.BiConsumer;

public interface Connection {

    <T> void submit(Protocol.Command command, BiConsumer<T, Throwable> callback, byte[][] arguments);

    void close();
}
