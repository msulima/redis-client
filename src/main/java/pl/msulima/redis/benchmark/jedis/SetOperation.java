package pl.msulima.redis.benchmark.jedis;

import pl.msulima.redis.benchmark.nio.Writer;
import redis.clients.jedis.Pipeline;

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
    public void run(Pipeline jedis) {
        jedis.set(key, value);
    }

    @Override
    public byte[] getBytes() {
        return ("SET " + new String(key) + " " + new String(value) + "\r\n").getBytes();
    }

    @Override
    public void writeTo(ByteBuffer byteBuffer) {
        Writer.sendCommand(byteBuffer, "SET", key, value);
    }

    @Override
    public void done() {
        callback.run();
    }

    @Override
    public void done(Object response) {
        done();
    }
}
