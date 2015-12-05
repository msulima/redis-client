package pl.msulima.redis.benchmark.io;

import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;

public interface Operation {

    void writeTo(RedisOutputStream outputStream);

    void readFrom(RedisInputStream inputStream);
}
