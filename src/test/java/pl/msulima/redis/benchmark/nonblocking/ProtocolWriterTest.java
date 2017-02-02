package pl.msulima.redis.benchmark.nonblocking;

import org.junit.Test;
import redis.clients.jedis.Protocol;
import redis.clients.util.RedisOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class ProtocolWriterTest {

    public static final int BUFFER_SIZE = 64;

    @Test
    public void checkSend() throws IOException {
        // given
        redis.clients.jedis.Protocol.Command command = redis.clients.jedis.Protocol.Command.SET;
        String expected = expectedSend(out -> Protocol.sendCommand(out, command));

        // when
        String result = resultSend(out -> ProtocolWriter.write(out, 0, command.raw));

        // then
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void checkSendWithArgs() throws IOException {
        // given
        redis.clients.jedis.Protocol.Command command = redis.clients.jedis.Protocol.Command.SET;
        String expected = expectedSend(out -> Protocol.sendCommand(out, command, new byte[]{1, 2, 3}, new byte[]{6, 7, 8}));

        // when
        String result = resultSend(out -> ProtocolWriter.write(out, 0, command.raw, new byte[]{1, 2, 3}, new byte[]{6, 7, 8}));

        // then
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void checkSendRejectsIfBufferToSmall() throws IOException {
        // given
        redis.clients.jedis.Protocol.Command command = redis.clients.jedis.Protocol.Command.SET;
        String expected = expectedSend(out -> {
            Protocol.sendCommand(out, command);
            Protocol.sendCommand(out, command, new byte[]{1, 2, 3});
        });

        // when
        String result = resultSend(out -> {
            int first = ProtocolWriter.write(out, 0, command.raw);
            int second = ProtocolWriter.write(out, first, command.raw, new byte[]{1, 2, 3});
            return ProtocolWriter.write(out, second, command.raw, new byte[]{1, 2, 3}, new byte[]{6, 7, 8});
        });

        // then
        assertThat(result).isEqualTo(expected);
    }

    private String resultSend(Function<byte[], Integer> consumer) {
        byte[] nioBytes = new byte[BUFFER_SIZE];
        int writtenBytes = consumer.apply(nioBytes);
        byte[] result = new byte[writtenBytes];
        System.arraycopy(nioBytes, 0, result, 0, writtenBytes);

        return new String(result);
    }

    private String expectedSend(Consumer<RedisOutputStream> consumer) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream(BUFFER_SIZE);
        RedisOutputStream redisOutputStream = new RedisOutputStream(stream);
        consumer.accept(redisOutputStream);
        try {
            redisOutputStream.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new String(stream.toByteArray());
    }
}
