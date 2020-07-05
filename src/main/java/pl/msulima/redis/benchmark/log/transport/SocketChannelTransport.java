package pl.msulima.redis.benchmark.log.transport;

import org.agrona.LangUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class SocketChannelTransport implements Transport {

    private final SocketChannel socketChannel;

    public SocketChannelTransport(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    @Override
    public void send(ByteBuffer buffer) {
        if (buffer.hasRemaining()) {
            try {
                socketChannel.write(buffer);
            } catch (IOException ex) {
                LangUtil.rethrowUnchecked(ex);
            }
        }
    }

    @Override
    public void receive(ByteBuffer buffer) {
        try {
            socketChannel.read(buffer);
        } catch (IOException ex) {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    @Override
    public void register(Selector selector, Object attachment) {
        try {
            socketChannel.register(selector, SelectionKey.OP_CONNECT, attachment);
        } catch (ClosedChannelException ex) {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    @Override
    public void connect() {
        while (socketChannel.isConnectionPending()) {
            try {
                socketChannel.finishConnect();
            } catch (IOException ex) {
                LangUtil.rethrowUnchecked(ex);
            }
        }
    }

    @Override
    public void close() {
        try {
            socketChannel.close();
        } catch (IOException ex) {
            LangUtil.rethrowUnchecked(ex);
        }
    }
}
