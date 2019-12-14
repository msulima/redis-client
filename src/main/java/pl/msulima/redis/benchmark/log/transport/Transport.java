package pl.msulima.redis.benchmark.log.transport;

import java.nio.ByteBuffer;
import java.nio.channels.Selector;

public interface Transport {

    void send(ByteBuffer buffer);

    void receive(ByteBuffer buffer);

    void register(Selector selector, Object attachment);

    void connect();
}
