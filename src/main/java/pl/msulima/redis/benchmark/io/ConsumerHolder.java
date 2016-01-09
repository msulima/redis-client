package pl.msulima.redis.benchmark.io;

import java.util.function.BiConsumer;

public class ConsumerHolder {
    private BiConsumer consumer;

    public void setConsumer(BiConsumer consumer) {
        this.consumer = consumer;
    }

    public BiConsumer getConsumer() {
        return consumer;
    }

}
