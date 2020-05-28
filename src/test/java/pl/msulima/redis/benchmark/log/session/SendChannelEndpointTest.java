package pl.msulima.redis.benchmark.log.session;

import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.junit.Test;
import pl.msulima.redis.benchmark.log.Request;
import pl.msulima.redis.benchmark.log.logbuffer.PublicationImage;
import pl.msulima.redis.benchmark.log.presentation.SenderAgent;
import pl.msulima.redis.benchmark.log.protocol.Command;
import pl.msulima.redis.benchmark.log.protocol.DynamicEncoder;
import pl.msulima.redis.benchmark.log.transport.RedisServerTransport;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SendChannelEndpointTest {

    private static final int WRITE_ITERATIONS = 100;
    private static final int DRAIN_ITERATIONS = WRITE_ITERATIONS * 2;
    private static final int SEND_BUFFER_SIZE = 256;

    private final SenderAgent sender = new SenderAgent(1);
    private final PublicationImage requestsImage = new PublicationImage(SEND_BUFFER_SIZE, DynamicEncoder.MAX_INTEGER_LENGTH);
    private final SendChannelEndpoint endpoint = new SendChannelEndpoint(requestsImage);
    private final RedisServerTransport transport = new RedisServerTransport();

    @Test
    public void shouldPassDataToTransport() {
        // given
        List<Request<String>> requests = new LinkedList<>();
        for (int i = 0; i < WRITE_ITERATIONS; i++) {
            Request<String> request = new Request<>(Command.INFO, Request::getSimpleString);
            requests.add(request);
        }
        ManyToOneConcurrentArrayQueue<Request<?>> requestsQueue = new ManyToOneConcurrentArrayQueue<>(WRITE_ITERATIONS);
        OneToOneConcurrentArrayQueue<Request<?>> callbacks = new OneToOneConcurrentArrayQueue<>(WRITE_ITERATIONS);
        sender.registerChannelEndpoint(requestsQueue, callbacks, requestsImage);

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
        List<Request<?>> actualCallbacks = new ArrayList<>();
        callbacks.drainTo(actualCallbacks, callbacks.size());
        assertThat(actualCallbacks).isEqualTo(requests);
    }
}
