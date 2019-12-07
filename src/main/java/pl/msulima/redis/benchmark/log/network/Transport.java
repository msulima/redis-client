package pl.msulima.redis.benchmark.log.network;

import java.nio.ByteBuffer;

public interface Transport {

    void receive(ByteBuffer buffer);
}
