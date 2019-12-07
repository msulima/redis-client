package pl.msulima.redis.benchmark.log.network;

import org.junit.Test;
import pl.msulima.redis.benchmark.log.ReceiveChannelEndpoint;
import pl.msulima.redis.benchmark.log.protocol.DecoderTest;
import pl.msulima.redis.benchmark.log.protocol.Response;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class RedisTransportPollerTest {

    @Test
    public void shouldReadResponses() {
        // given
        RedisTransportPoller transportPoller = new RedisTransportPoller(1024);
        ByteBuffer src = ByteBuffer.allocate(1024)
                .put(DecoderTest.encodeSimpleString("ok1"))
                .put(DecoderTest.encodeSimpleString("ok2"))
                .flip();
        ByteBufferTransport transport = new ByteBufferTransport(src);
        ReceiveChannelEndpoint endpoint = new ReceiveChannelEndpoint();
        transportPoller.registerForRead(endpoint, transport);

        // when
        transportPoller.pollTransports();

        // then
        assertThat(endpoint.responses).containsExactly(Response.simpleString("ok1"), Response.simpleString("ok2"));
    }
}