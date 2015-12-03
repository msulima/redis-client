package pl.msulima.redis.benchmark.jedis;

import pl.msulima.redis.benchmark.nio.Writer;
import redis.clients.jedis.Pipeline;

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
    public void run(Pipeline jedis) {
        jedis.del(key);
    }

    @Override
    public byte[] getBytes() {
        return ("DEL " + new String(key) + "\r\n").getBytes();
    }

    @Override
    public void writeTo(ByteBuffer byteBuffer) {
        Writer.sendCommand(byteBuffer, "DEL", key);
    }

    @Override
    public void done() {
        callback.accept(1);
    }

    @Override
    public void done(Object response) {
        callback.accept((Integer) response);
    }
}
