package pl.msulima.redis.benchmark.jedis;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

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

    @SuppressWarnings("unchecked")
    @Override
    public void done() {
        callback.accept(response.get());
    }
}
