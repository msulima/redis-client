package pl.msulima.redis.benchmark.log.logbuffer;

import java.nio.ByteBuffer;

class BufferClaim {

    private byte[] buffer;
    private int position;
    private int capacity;

    void wrap(byte[] buffer, int position, int capacity) {
        this.buffer = buffer;
        this.position = position;
        this.capacity = capacity;
    }

    ByteBuffer getByteBuffer() {
        return ByteBuffer.wrap(buffer, position, capacity);
    }

    void commit() {
        getByteBuffer().putInt(capacity); // TODO volatile!
    }
}
