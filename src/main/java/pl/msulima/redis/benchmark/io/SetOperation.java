package pl.msulima.redis.benchmark.io;

import redis.clients.jedis.Protocol;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;

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
    public void writeTo(RedisOutputStream outputStream) {
        Protocol.sendCommand(outputStream, Protocol.Command.SET, key, value);
    }

    @Override
    public void readFrom(RedisInputStream inputStream) {
        done(Protocol.read(inputStream));
    }

    private void done(Object response) {
        callback.run();
    }
}
