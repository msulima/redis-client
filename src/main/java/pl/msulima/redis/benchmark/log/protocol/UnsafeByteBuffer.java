package pl.msulima.redis.benchmark.log.protocol;

import java.nio.ByteBuffer;

import static org.agrona.BufferUtil.*;
import static org.agrona.UnsafeAccess.UNSAFE;

public class UnsafeByteBuffer {

    private static final int JNI_COPY_FROM_ARRAY_THRESHOLD = 6;
    private int limit;
    private int position;
    private byte[] byteArray;
    private long addressOffset;

    public UnsafeByteBuffer(ByteBuffer buffer) {
        this.limit = buffer.limit();
        this.position = buffer.position();

        if (buffer.isDirect()) {
            this.byteArray = null;
            this.addressOffset = address(buffer);
        } else {
            this.byteArray = array(buffer);
            this.addressOffset = ARRAY_BASE_OFFSET + arrayOffset(buffer);
        }
    }

    public void put(byte[] src) {
        put(src, 0, src.length);
    }

    public void put(byte[] src, int offset, int length) {
        if (length > JNI_COPY_FROM_ARRAY_THRESHOLD) {
            UNSAFE.copyMemory(src, ARRAY_BASE_OFFSET + offset, byteArray, addressOffset + position, length);
        } else {
            for (int i = 0; i < length; i++) {
                put(position + i, src[offset + i]);
            }
        }
        position += length;
    }

    public void put(int index, byte b) {
        UNSAFE.putByte(byteArray, addressOffset + index, b);
    }

    public void put(byte b) {
        UNSAFE.putByte(byteArray, addressOffset + position, b);
        position++;
    }

    public int remaining() {
        return limit - position;
    }

    public int position() {
        return position;
    }

    public void position(int position) {
        this.position = position;
    }
}
