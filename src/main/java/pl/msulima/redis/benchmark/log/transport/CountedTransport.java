package pl.msulima.redis.benchmark.log.transport;

import org.agrona.concurrent.status.AtomicCounter;

import java.nio.ByteBuffer;
import java.nio.channels.Selector;

public class CountedTransport implements Transport {

    private final Transport delegate;
    private final AtomicCounter sendSize;

    public CountedTransport(Transport delegate, AtomicCounter sendSize) {
        this.delegate = delegate;
        this.sendSize = sendSize;
    }

    @Override
    public void send(ByteBuffer buffer) {
        sendSize.getAndAddOrdered(buffer.remaining());
        delegate.send(buffer);
    }

    @Override
    public void receive(ByteBuffer buffer) {
        delegate.receive(buffer);
    }

    @Override
    public void register(Selector selector, Object attachment) {
        delegate.register(selector, attachment);
    }

    @Override
    public void connect() {
        delegate.connect();
    }
}
