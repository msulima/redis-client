package pl.msulima.redis.benchmark.jedis;

import pl.msulima.redis.benchmark.nio.Encoder;
import pl.msulima.redis.benchmark.nio.Writer;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Consumer;

public class PingOperation implements Operation {

    private final Consumer<String> callback;
    private Response<String> response;
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
    public void run(Pipeline jedis) {
        response = jedis.ping();
    }

    @Override
    public byte[] getBytes() {
        return text.map(t -> ("PING \"" + t.replace("\"", "\\\"") + "\"\r\n").getBytes()).orElse(("PING\r\n").getBytes());
    }

    @Override
    public void writeTo(ByteBuffer byteBuffer) {
        if (text.isPresent()) {
            Writer.sendCommand(byteBuffer, "PING", Encoder.encode(text.get()));
        } else {
            Writer.sendCommand(byteBuffer, "PING");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void done() {
        callback.accept(response.get());
    }

    @Override
    public void done(Object response) {
        callback.accept(text.map(t -> new String(((Optional<byte[]>) response).get())).orElseGet(() -> (String) response));
    }
}
