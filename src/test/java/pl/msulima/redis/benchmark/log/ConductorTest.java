package pl.msulima.redis.benchmark.log;

import org.junit.Before;
import org.junit.Test;
import pl.msulima.redis.benchmark.log.presentation.ReceiverAgent;
import pl.msulima.redis.benchmark.log.presentation.SenderAgent;
import pl.msulima.redis.benchmark.log.session.RedisTransportPoller;
import pl.msulima.redis.benchmark.log.transport.RedisServerTransport;
import pl.msulima.redis.benchmark.log.transport.TransportFactory;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConductorTest {

    private static final int WRITE_ITERATIONS = 1000;
    private static final int DRAIN_ITERATIONS = WRITE_ITERATIONS * 3;
    private static final InetSocketAddress LOCALHOST = new InetSocketAddress("localhost", 6379);
    private static final int USE_SELECTOR_THRESHOLD = 2;
    private static final int COMMAND_QUEUE_SIZE = 8;
    private static final int REQUESTS_QUEUE_SIZE = WRITE_ITERATIONS;
    private static final int BUFFER_SIZE = 32;

    private final TransportFactory transportFactory = mock(TransportFactory.class);
    private final RedisServerTransport transport1 = new RedisServerTransport();
    private final RedisServerTransport transport2 = new RedisServerTransport();
    private final NetworkAgent networkAgent = new NetworkAgent(new RedisTransportPoller(USE_SELECTOR_THRESHOLD), COMMAND_QUEUE_SIZE);
    private final SenderAgent senderAgent = new SenderAgent(BUFFER_SIZE, COMMAND_QUEUE_SIZE);
    private final ReceiverAgent receiverAgent = new ReceiverAgent(COMMAND_QUEUE_SIZE);
    private final Conductor conductor = new Conductor(transportFactory, networkAgent, senderAgent, receiverAgent, COMMAND_QUEUE_SIZE, REQUESTS_QUEUE_SIZE, BUFFER_SIZE);

    private ClientFacade[] clientFacades;

    @Before
    public void setUp() throws InterruptedException, ExecutionException, TimeoutException {
        when(transportFactory.forAddress(LOCALHOST, BUFFER_SIZE)).thenReturn(transport1, transport2);
        CompletionStage<ClientFacade> connection1 = conductor.connect(LOCALHOST);
        CompletionStage<ClientFacade> connection2 = conductor.connect(LOCALHOST);
        conductor.doWork();
        clientFacades = new ClientFacade[]{
                connection1.toCompletableFuture().get(1, TimeUnit.SECONDS),
                connection2.toCompletableFuture().get(1, TimeUnit.SECONDS)
        };
    }

    @Test
    public void shouldReadResponses() {
        // given
        List<CompletionStage<String>> requests = new LinkedList<>();

        // when
        for (int i = 0; i < DRAIN_ITERATIONS; i++) {
            if (i < WRITE_ITERATIONS) {
                requests.add(clientFacades[i % clientFacades.length].ping(generateString(i)));
            }
            conductor.doWork();
            networkAgent.doWork();
            senderAgent.doWork();
            receiverAgent.doWork();
            transport1.processRequests();
            transport2.processRequests();
        }

        // then
        for (int i = 0; i < WRITE_ITERATIONS; i++) {
            assertThat(requests.get(i)).isCompletedWithValue(generateString(i));
        }
    }

    private String generateString(int i) {
        return "x".repeat(i % 100) + "-" + i;
    }
}
