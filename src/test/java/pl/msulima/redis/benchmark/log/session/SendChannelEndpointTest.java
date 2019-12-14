package pl.msulima.redis.benchmark.log.session;

import org.junit.Test;
import pl.msulima.redis.benchmark.log.Request;
import pl.msulima.redis.benchmark.log.transport.RedisServerTransport;
import redis.clients.jedis.Protocol;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

public class SendChannelEndpointTest {

    private static final int WRITE_ITERATIONS = 100;
    private static final int DRAIN_ITERATIONS = WRITE_ITERATIONS * 2;
    private static final int SEND_BUFFER_SIZE = 1024;

    @Test
    public void shouldPassDataToTransport() {
        // given
        List<Request<String>> requests = new LinkedList<>();
        for (int i = 0; i < WRITE_ITERATIONS; i++) {
            Request<String> request = new Request<>(Protocol.Command.INFO, Request::getSimpleString);
            requests.add(request);
        }
        Queue<Request<?>> requestsQueue = new LinkedList<>();
        Queue<Request<?>> callbacksQueue = new LinkedList<>();
        SendChannelEndpoint endpoint = new SendChannelEndpoint(requestsQueue, callbacksQueue, SEND_BUFFER_SIZE);
        RedisServerTransport transport = new RedisServerTransport();

        // when
        for (int i = 0; i < DRAIN_ITERATIONS; i++) {
            if (i < WRITE_ITERATIONS) {
                requestsQueue.add(requests.get(i));
            }
            endpoint.send(transport);
        }

        // then
        List<Request<?>> results = transport.getRequests();
        for (int i = 0; i < WRITE_ITERATIONS; i++) {
            assertThat(results.get(i).command).isEqualTo(requests.get(i).command);
            assertThat(results.get(i).args).isEqualTo(requests.get(i).args);
        }
        assertThat(callbacksQueue).isEqualTo(requests);
    }
}
