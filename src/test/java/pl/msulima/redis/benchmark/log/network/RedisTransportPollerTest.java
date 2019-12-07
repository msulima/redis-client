package pl.msulima.redis.benchmark.log.network;

import org.junit.Test;
import pl.msulima.redis.benchmark.log.Command;
import pl.msulima.redis.benchmark.log.ReceiveChannelEndpoint;
import pl.msulima.redis.benchmark.log.protocol.DecoderTest;
import redis.clients.jedis.Protocol;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;

import static org.assertj.core.api.Assertions.assertThat;

public class RedisTransportPollerTest {

    private final RedisTransportPoller transportPoller = new RedisTransportPoller(1024);

    @Test
    public void shouldReadResponses() {
        // given
        Command<String> first = new Command<>(Protocol.Command.INFO, Command::decodeSimpleString);
        Command<String> second = new Command<>(Protocol.Command.INFO, Command::decodeSimpleString);
        ReceiveChannelEndpoint endpoint = new ReceiveChannelEndpoint(new LinkedList<>(Arrays.asList(first, second)));
        ByteBuffer src = ByteBuffer.allocate(1024)
                .put(DecoderTest.encodeSimpleString("ok1"))
                .put(DecoderTest.encodeSimpleString("ok2"))
                .flip();
        ByteBufferTransport transport = new ByteBufferTransport(src);

        // when
        transportPoller.registerForRead(endpoint, transport);
        transportPoller.pollTransports();

        // then
        assertThat(first.getPromise().toCompletableFuture()).isCompletedWithValue("ok1");
        assertThat(second.getPromise().toCompletableFuture()).isCompletedWithValue("ok2");
    }
}
