package pl.msulima.redis.benchmark.jedis;

import redis.clients.jedis.Pipeline;

import java.nio.ByteBuffer;

public interface Operation {

    void run(Pipeline jedis);

    byte[] getBytes();

    void writeTo(ByteBuffer byteBuffer);

    void done();

    void done(Object response);
}
