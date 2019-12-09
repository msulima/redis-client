package pl.msulima.redis.benchmark.log.network;

import java.nio.ByteBuffer;

public interface Transport {

    void send(ByteBuffer buffer);

    void receive(ByteBuffer buffer);
}
