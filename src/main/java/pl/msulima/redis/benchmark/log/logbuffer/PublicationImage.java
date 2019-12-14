package pl.msulima.redis.benchmark.log.logbuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

class PublicationImage {

    private final byte[] buffer;
    private final int capacity;
    private long head = 0;
    private AtomicLong tailCommitted = new AtomicLong();
    private AtomicLong tailReserved = new AtomicLong();

    PublicationImage(int capacity) {
        this.capacity = capacity;
        this.buffer = new byte[capacity];
    }

    void writeClaim(int capacity, BufferClaim bufferClaim) {
        long position = tailReserved.getAndAdd(capacity);
        bufferClaim.wrap(buffer, sequenceToBufferOffset(position), capacity);
    }

    int remaining() {
        ByteBuffer wrap = ByteBuffer.wrap(buffer);
        int total = 0;
        int frameLength;
        do {
            frameLength = wrap.getInt(sequenceToBufferOffset(head + total));
            total += frameLength;
        } while (frameLength > 0);
        return total;
    }

    long tail() {
        return tailCommitted.get();
    }

    long head() {
        return head;
    }

    private int sequenceToBufferOffset(long sequence) {
        return (int) (sequence % capacity);
    }
}
