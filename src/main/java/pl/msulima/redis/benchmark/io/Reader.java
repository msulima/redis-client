package pl.msulima.redis.benchmark.io;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.RedisInputStream;

import java.io.InputStream;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

public class Reader {

    private final RedisInputStream redisInputStream;
    private final RingBuffer<ConsumerHolder> ringBuffer;

    public Reader(InputStream inputStream, int bufferSize) {
        redisInputStream = new RedisInputStream(inputStream, bufferSize);

        Executor executor = Executors.newFixedThreadPool(8);
        Disruptor<ConsumerHolder> disruptor = new Disruptor<>(ConsumerHolder::new, 512 * 1024, executor,
                ProducerType.SINGLE, new BlockingWaitStrategy());

        //noinspection unchecked
        disruptor.handleEventsWith(this::handleEvent);
        disruptor.start();

        ringBuffer = disruptor.getRingBuffer();
    }

    public void read(BiConsumer callback) {
        ringBuffer.publishEvent(Reader::translate, callback);
    }

    private static void translate(ConsumerHolder event, long sequence, BiConsumer callback) {
        event.setConsumer(callback);
    }

    private void handleEvent(ConsumerHolder command, long sequence, boolean endOfBatch) {
        try {
            Object read = Protocol.read(redisInputStream);
            //noinspection unchecked
            command.getConsumer().accept(read, null);
        } catch (JedisException ex) {
            command.getConsumer().accept(null, ex);
        }
    }
}
