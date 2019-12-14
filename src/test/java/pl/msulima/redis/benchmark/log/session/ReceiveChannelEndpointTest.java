package pl.msulima.redis.benchmark.log.session;

import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.junit.Test;
import pl.msulima.redis.benchmark.log.Request;
import pl.msulima.redis.benchmark.log.presentation.ReceiverAgent;
import pl.msulima.redis.benchmark.log.protocol.Command;
import pl.msulima.redis.benchmark.log.transport.RedisServerTransport;

import java.util.LinkedList;
import java.util.List;

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
            Request<String> request = new Request<>(Command.INFO, Request::getSimpleString);
            requests.add(request);
        }
        OneToOneConcurrentArrayQueue<byte[]> responses = new OneToOneConcurrentArrayQueue<>(WRITE_ITERATIONS);
        OneToOneConcurrentArrayQueue<Request<?>> callbacksQueue = new OneToOneConcurrentArrayQueue<>(WRITE_ITERATIONS);
        ReceiveChannelEndpoint endpoint = new ReceiveChannelEndpoint(RECEIVE_BUFFER_SIZE, responses);
        RedisServerTransport transport = new RedisServerTransport();
        ReceiverAgent receiver = new ReceiverAgent(1);
        receiver.registerChannelEndpoint(responses, callbacksQueue);

        // when
        for (int i = 0; i < DRAIN_ITERATIONS; i++) {
            if (i < WRITE_ITERATIONS) {
                callbacksQueue.offer(requests.get(i));
                transport.insertSimpleStringResponse(generateString(i));
            }
            endpoint.receive(transport);
            int workDone;
            do {
                workDone = receiver.doWork();
            } while (workDone > 0);
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
