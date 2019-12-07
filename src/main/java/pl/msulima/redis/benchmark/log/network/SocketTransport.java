package pl.msulima.redis.benchmark.log.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

class SocketTransport implements Transport {

    private SocketChannel socketChannel;

    @Override
    public void receive(ByteBuffer buffer) {
        buffer.clear();
        try {
            socketChannel.read(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
