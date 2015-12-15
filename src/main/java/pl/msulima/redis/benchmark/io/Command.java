package pl.msulima.redis.benchmark.io;

import redis.clients.jedis.Protocol;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;

import java.util.concurrent.CompletableFuture;

public interface Command {

    Protocol.Command getCommand();

    byte[][] getArguments();

    CompletableFuture<?> getCallback();

    void writeTo(RedisOutputStream outputStream);

    void readFrom(RedisInputStream inputStream);
}
