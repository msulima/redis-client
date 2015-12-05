package pl.msulima.redis.benchmark.io;

import redis.clients.jedis.Protocol;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;
import redis.clients.util.SafeEncoder;

import java.util.Optional;
import java.util.function.Consumer;

public class PingOperation implements Operation {

    private final Consumer<String> callback;
    private final Optional<String> text;

    public PingOperation(String text, Consumer<String> callback) {
        this.text = Optional.of(text);
        this.callback = callback;
    }

    public PingOperation(Consumer<String> callback) {
        this.text = Optional.empty();
        this.callback = callback;
    }

    @Override
    public void writeTo(RedisOutputStream outputStream) {
        if (text.isPresent()) {
            Protocol.sendCommand(outputStream, Protocol.Command.PING, SafeEncoder.encode(text.get()));
        } else {
            Protocol.sendCommand(outputStream, Protocol.Command.PING);
        }
    }

    @Override
    public void readFrom(RedisInputStream inputStream) {
        done(Protocol.read(inputStream));
    }

    @SuppressWarnings("unchecked")
    private void done(Object response) {
        callback.accept(new String((byte[]) response));
    }
}
