package pl.msulima.redis.benchmark.io;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.RedisOutputStream;

import java.io.OutputStream;
import java.net.SocketException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

public class Writer {

    private final RedisOutputStream redisOutputStream;
    private final RingBuffer<CommandHolder> ringBuffer;
    private final Reader reader;

    public Writer(OutputStream outputStream, Reader reader, int bufferSize) {
        this.redisOutputStream = new RedisOutputStream(outputStream, bufferSize);
        this.reader = reader;

        Executor executor = Executors.newFixedThreadPool(8);
        Disruptor<CommandHolder> disruptor = new Disruptor<>(CommandHolder::new, 512 * 1024, executor);

        //noinspection unchecked
        disruptor.handleEventsWith(this::handleEvent);
        disruptor.start();

        ringBuffer = disruptor.getRingBuffer();
    }

    public <T> void write(Protocol.Command command, BiConsumer<T, Throwable> callback, byte[][] arguments) {
        ringBuffer.publishEvent(Writer::translate, command, callback, arguments);
    }

    private static <T> void translate(CommandHolder event, long sequence, Protocol.Command command,
                                      BiConsumer<T, Throwable> callback, byte[][] arguments) {
        event.setCommand(command);
        event.setArguments(arguments);
        event.setCallback(callback);
    }

    private void handleEvent(CommandHolder<?> command, long sequence, boolean endOfBatch) {
        writeOne(this.reader, command);

        if (endOfBatch) {
            try {
                redisOutputStream.flush();
            } catch (JedisException | SocketException e) {
                command.getCallback().accept(null, e);
            } catch (Exception e) {
                command.getCallback().accept(null, e);
                throw new RuntimeException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void writeOne(Reader reader, CommandHolder command) {
        try {
            Protocol.sendCommand(redisOutputStream, command.getCommand(), command.getArguments());
        } catch (JedisException e) {
            command.getCallback().accept(null, e);
        } catch (Exception e) {
            command.getCallback().accept(null, e);
            throw new RuntimeException(e);
        }
        reader.read(command.getCallback());
    }
}