package pl.msulima.redis.benchmark.io;

import redis.clients.jedis.Protocol;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;

import java.util.function.Consumer;

public class DelOperation implements Operation {

    private final byte[] key;
    private final Consumer<Long> callback;

    public DelOperation(byte[] key, Consumer<Long> callback) {
        this.key = key;
        this.callback = callback;
    }

    @Override
    public void writeTo(RedisOutputStream outputStream) {
        Protocol.sendCommand(outputStream, Protocol.Command.DEL, key);
    }

    @Override
    public void readFrom(RedisInputStream inputStream) {
        done(Protocol.read(inputStream));
    }

    private void done(Object response) {
        callback.accept((Long) response);
    }
}
