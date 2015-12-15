package pl.msulima.redis.benchmark.io;

import redis.clients.util.RedisOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Writer {

    private final RedisOutputStream redisOutputStream;
    private final BlockingQueue<Command> commands;

    public Writer(OutputStream outputStream, Reader reader, int bufferSize) {
        redisOutputStream = new RedisOutputStream(outputStream, bufferSize);
        commands = new ArrayBlockingQueue<>(1024 * 1024);

        new Thread(() -> {
            try {
                Command command;
                while ((command = commands.take()) != null) {
                    writeOne(reader, command);
                    while ((command = commands.poll()) != null) {
                        writeOne(reader, command);
                    }
                    redisOutputStream.flush();
                }
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }

    private void writeOne(Reader reader, Command command) {
        command.writeTo(redisOutputStream);
        reader.read(command);
    }

    public void write(Command command) {
        commands.add(command);
    }
}