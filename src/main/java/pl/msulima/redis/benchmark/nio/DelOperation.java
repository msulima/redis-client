package pl.msulima.redis.benchmark.nio;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class DelOperation implements Operation {

    private final byte[] key;
    private final Consumer<Integer> callback;

    public DelOperation(byte[] key, Consumer<Integer> callback) {
        this.key = key;
        this.callback = callback;
    }

    @Override
    public void writeTo(ByteBuffer byteBuffer) {
        Writer.sendCommand(byteBuffer, "DEL", key);
    }

    @Override
    public void done(Object response) {
        callback.accept((Integer) response);
    }
}
