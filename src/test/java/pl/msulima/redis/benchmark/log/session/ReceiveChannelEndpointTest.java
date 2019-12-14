package pl.msulima.redis.benchmark.log.session;

import org.junit.Test;
import pl.msulima.redis.benchmark.log.Request;
import pl.msulima.redis.benchmark.log.transport.RedisServerTransport;
import redis.clients.jedis.Protocol;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

public class ReceiveChannelEndpointTest {

    private static final int WRITE_ITERATIONS = 100;
    private static final int DRAIN_ITERATIONS = WRITE_ITERATIONS * 2;
    private static final int RECEIVE_BUFFER_SIZE = 1024;

    @Test
    public void shouldReadResponses() {
        // given
        List<Request<String>> requests = new LinkedList<>();
        for (int i = 0; i < WRITE_ITERATIONS; i++) {
            Request<String> request = new Request<>(Protocol.Command.INFO, Request::getSimpleString);
            requests.add(request);
        }
        Queue<Request<?>> callbacksQueue = new LinkedList<>();
        ReceiveChannelEndpoint endpoint = new ReceiveChannelEndpoint(callbacksQueue, RECEIVE_BUFFER_SIZE);
        RedisServerTransport transport = new RedisServerTransport();

        // when
        for (int i = 0; i < DRAIN_ITERATIONS; i++) {
            if (i < WRITE_ITERATIONS) {
                callbacksQueue.offer(requests.get(i));
                transport.insertSimpleStringResponse(generateString(i));
            }
            endpoint.receive(transport);
        }

        // then
        for (int i = 0; i < WRITE_ITERATIONS; i++) {
            assertThat(requests.get(i).getPromise()).isCompletedWithValue(generateString(i));
        }
    }

    private String generateString(int i) {
        return "x".repeat(RedisServerTransport.NETWORK_BUFFER_SIZE) + "-" + i;
    }
}
