package pl.msulima.redis.benchmark.io;

import redis.clients.util.RedisInputStream;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.io.InputStream;
import java.util.concurrent.locks.LockSupport;

public class Reader {

    private final RedisInputStream redisInputStream;
    private final OneToOneConcurrentArrayQueue<Command> commands;

    public Reader(InputStream inputStream, int bufferSize) {
        redisInputStream = new RedisInputStream(inputStream, bufferSize);
        commands = new OneToOneConcurrentArrayQueue<>(1024 * 1024);

        new Thread(() -> {
            while (true) {
                commands.drain(command -> {
                    command.readFrom(redisInputStream);
                });
                LockSupport.parkNanos(0);
            }
        }).start();
    }

    public void read(Command command) {
        commands.add(command);
    }
}
