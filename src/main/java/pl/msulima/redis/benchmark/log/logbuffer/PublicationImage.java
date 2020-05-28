package pl.msulima.redis.benchmark.log.logbuffer;

import java.nio.ByteBuffer;

public class PublicationImage {

    private final byte[] buffer;
    private final int margin;
    private final int capacity;
    private volatile long head = 0;
    private volatile long tail = 0;

    PublicationImage(int capacity) {
        this(capacity, 0);
    }

    public PublicationImage(int capacity, int margin) {
        this.capacity = capacity;
        this.buffer = new byte[capacity];
        this.margin = margin;
    }

    public ByteBuffer writeClaim() {
        long localHead = this.head;
        long localTail = this.tail;
        int offsetHead = sequenceToBufferOffset(localHead);
        int offsetTail = sequenceToBufferOffset(localTail);

        long remaining = capacity - (localTail - localHead);
        if (remaining == 0 || remaining < margin) {
            return ByteBuffer.wrap(buffer, offsetTail, 0);
        }
        if (offsetHead <= offsetTail) {
            int remainingAtEndOfArray = capacity - offsetTail;
            if (remainingAtEndOfArray < margin) {
                return ByteBuffer.allocate(margin);
            }
            return ByteBuffer.wrap(buffer, offsetTail, remainingAtEndOfArray);
        }
        return ByteBuffer.wrap(buffer, offsetTail, offsetHead - offsetTail);
    }

    public ByteBuffer readClaim() {
        long localHead = this.head;
        long localTail = this.tail;
        int offsetHead = sequenceToBufferOffset(localHead);
        int offsetTail = sequenceToBufferOffset(localTail);
        if (offsetHead <= offsetTail) {
            return ByteBuffer.wrap(buffer, offsetHead, (int) (localTail - localHead));
        }
        return ByteBuffer.wrap(buffer, offsetHead, capacity - offsetHead);
    }

    public void commitWrite(ByteBuffer buffer) {
        long localTail = this.tail;
        int offsetTail = sequenceToBufferOffset(localTail);
        int written;
        if (buffer.array() == this.buffer) {
            written = buffer.position() - offsetTail;
        } else {
            buffer.flip();
            int remainingAtEndOfArray = capacity - offsetTail;
            buffer.get(this.buffer, offsetTail, Math.min(buffer.remaining(), remainingAtEndOfArray));
            buffer.get(this.buffer, 0, buffer.remaining());
            written = buffer.position();
        }
        this.tail = localTail + written;
    }

    public void commitRead(ByteBuffer buffer) {
        long localHead = this.head;
        int offsetHead = sequenceToBufferOffset(localHead);
        this.head = localHead + buffer.position() - offsetHead;
    }

    private int sequenceToBufferOffset(long sequence) {
        return (int) (sequence % capacity);
    }
}
