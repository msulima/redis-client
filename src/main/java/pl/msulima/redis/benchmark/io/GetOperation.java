package pl.msulima.redis.benchmark.io;

import redis.clients.jedis.Protocol;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;

import java.util.function.Consumer;

public class GetOperation implements Operation {

    private final byte[] key;
    private final Consumer<byte[]> callback;

    public GetOperation(byte[] key, Consumer<byte[]> callback) {
        this.key = key;
        this.callback = callback;
    }

    @Override
    public void writeTo(RedisOutputStream outputStream) {
        Protocol.sendCommand(outputStream, Protocol.Command.GET, key);
    }

    @Override
    public void readFrom(RedisInputStream inputStream) {
        done(Protocol.read(inputStream));
    }

    private void done(Object response) {
        callback.accept((byte[]) response);
    }
}
