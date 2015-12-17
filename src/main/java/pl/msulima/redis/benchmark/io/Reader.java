package pl.msulima.redis.benchmark.io;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import redis.clients.util.RedisInputStream;

import java.io.InputStream;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class Reader {

    private final RedisInputStream redisInputStream;
    private final RingBuffer<MutableResultCommand> ringBuffer;

    public Reader(InputStream inputStream, int bufferSize) {
        redisInputStream = new RedisInputStream(inputStream, bufferSize);

        Executor executor = Executors.newFixedThreadPool(8);
        Disruptor<MutableResultCommand> disruptor = new Disruptor<>(MutableResultCommand::new, 512 * 1024, executor,
                ProducerType.SINGLE, new BlockingWaitStrategy());

        //noinspection unchecked
        disruptor.handleEventsWith(this::handleEvent);
        disruptor.start();

        ringBuffer = disruptor.getRingBuffer();
    }

    public void read(Command command) {
        ringBuffer.publishEvent(Reader::translate, command);
    }

    private static void translate(MutableResultCommand event, long sequence, Command command) {
        event.setCommand(command.getCommand());
        event.setArguments(command.getArguments());
        event.setCallback(command.getCallback());
    }

    private void handleEvent(MutableResultCommand command, long sequence, boolean endOfBatch) {
        command.readFrom(redisInputStream);
    }
}
