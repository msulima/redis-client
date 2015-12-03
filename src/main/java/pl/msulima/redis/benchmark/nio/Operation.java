package pl.msulima.redis.benchmark.nio;

import java.nio.ByteBuffer;

public interface Operation {

    void writeTo(ByteBuffer byteBuffer);

    void done(Object response);
}
