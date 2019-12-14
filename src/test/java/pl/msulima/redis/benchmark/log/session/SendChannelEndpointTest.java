package pl.msulima.redis.benchmark.log.session;

import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.junit.Test;
import pl.msulima.redis.benchmark.log.Request;
import pl.msulima.redis.benchmark.log.presentation.SenderAgent;
import pl.msulima.redis.benchmark.log.protocol.Command;
import pl.msulima.redis.benchmark.log.transport.RedisServerTransport;

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
            Request<String> request = new Request<>(Command.INFO, Request::getSimpleString);
            requests.add(request);
        }
        Queue<Request<?>> requestsQueue = new LinkedList<>();
        Queue<Request<?>> callbacks = new LinkedList<>();
        OneToOneConcurrentArrayQueue<byte[]> serializedRequests = new OneToOneConcurrentArrayQueue<>(777);
        SenderAgent sender = new SenderAgent(SEND_BUFFER_SIZE, 1);
        sender.registerChannelEndpoint(requestsQueue, serializedRequests, callbacks);
        SendChannelEndpoint endpoint = new SendChannelEndpoint(serializedRequests);
        RedisServerTransport transport = new RedisServerTransport();

        // when
        for (int i = 0; i < DRAIN_ITERATIONS; i++) {
            if (i < WRITE_ITERATIONS) {
                requestsQueue.add(requests.get(i));
            }
            endpoint.send(transport);
            int workDone;
            do {
                workDone = sender.doWork();
            } while (workDone > 0);
        }

        // then
        List<Request<?>> results = transport.getRequests();
        for (int i = 0; i < WRITE_ITERATIONS; i++) {
            assertThat(results.get(i).command).isEqualTo(requests.get(i).command);
            assertThat(results.get(i).args).isEqualTo(requests.get(i).args);
        }
        assertThat(callbacks).isEqualTo(requests);
    }
}
