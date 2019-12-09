package pl.msulima.redis.benchmark.log.network;

import java.nio.ByteBuffer;

class ByteBufferTransport implements Transport {

    private final ByteBuffer src;

    ByteBufferTransport(ByteBuffer src) {
        this.src = src;
    }

    @Override
    public void send(ByteBuffer buffer) {
        src
                .clear()
                .put(buffer)
                .flip();
    }

    @Override
    public void receive(ByteBuffer buffer) {
        buffer
                .clear()
                .put(src)
                .flip();
    }
}
