package pl.msulima.redis.benchmark.io;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import redis.clients.util.RedisOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

public class Writer {

    private final RedisOutputStream redisOutputStream;
    private final RingBuffer<MutableResultCommand> ringBuffer;
    private final Reader reader;

    public Writer(OutputStream outputStream, Reader reader, int bufferSize) {
        this.redisOutputStream = new RedisOutputStream(outputStream, bufferSize);
        this.reader = reader;

        Executor executor = Executors.newFixedThreadPool(8);
        Disruptor<MutableResultCommand> disruptor = new Disruptor<>(MutableResultCommand::new, 1024, executor);

        //noinspection unchecked
        disruptor.handleEventsWith(this::handleEvent);
        disruptor.start();

        ringBuffer = disruptor.getRingBuffer();
    }

    public void write(Command command) {
        while (!ringBuffer.tryPublishEvent(Writer::translate, command)) {
            LockSupport.parkNanos(100);
        }
    }

    private static void translate(MutableResultCommand event, long sequence, Command command) {
        event.setCommand(command.getCommand());
        event.setArguments(command.getArguments());
        event.setCallback(command.getCallback());
    }

    private void handleEvent(MutableResultCommand command, long sequence, boolean endOfBatch) {
        writeOne(this.reader, command);

        if (endOfBatch) {
            try {
                redisOutputStream.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void writeOne(Reader reader, Command command) {
        command.writeTo(redisOutputStream);
        reader.read(new ResultCommand(command.getCommand(), command.getCallback(), command.getArguments()));
    }
}