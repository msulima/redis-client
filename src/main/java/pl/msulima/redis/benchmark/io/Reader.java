package pl.msulima.redis.benchmark.io;

import redis.clients.util.RedisInputStream;

import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Reader {

    private final RedisInputStream redisInputStream;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public Reader(InputStream inputStream) {
        redisInputStream = new RedisInputStream(inputStream);
    }

    public void read(Operation command) {
        executorService.execute(() -> {
            command.readFrom(redisInputStream);
        });
    }
}
