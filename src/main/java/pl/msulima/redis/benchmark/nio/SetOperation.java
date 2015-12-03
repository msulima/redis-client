package pl.msulima.redis.benchmark.nio;

import java.nio.ByteBuffer;

public class SetOperation implements Operation {

    private final byte[] key;
    private final byte[] value;
    private final Runnable callback;

    public SetOperation(byte[] key, byte[] value, Runnable callback) {
        this.key = key;
        this.value = value;
        this.callback = callback;
    }

    @Override
    public void writeTo(ByteBuffer byteBuffer) {
        Writer.sendCommand(byteBuffer, "SET", key, value);
    }

    @Override
    public void done(Object response) {
        callback.run();
    }
}
