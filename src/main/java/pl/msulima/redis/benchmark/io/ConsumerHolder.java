package pl.msulima.redis.benchmark.io;

import java.util.function.Consumer;

public class ConsumerHolder {
    private Consumer consumer;
    private Consumer<Throwable> onError;

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void setOnError(Consumer<Throwable> onError) {
        this.onError = onError;
    }

    public Consumer<Throwable> getOnError() {
        return onError;
    }
}
