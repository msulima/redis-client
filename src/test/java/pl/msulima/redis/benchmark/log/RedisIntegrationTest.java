package pl.msulima.redis.benchmark.log;

import org.agrona.LangUtil;
import org.junit.Before;
import org.junit.Test;
import pl.msulima.redis.benchmark.log.transport.SocketChannelTransportFactory;
import pl.msulima.redis.benchmark.log.transport.TransportFactory;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class RedisIntegrationTest {

    private static final int WRITE_ITERATIONS = 1000;
    private static final int DRAIN_ITERATIONS = WRITE_ITERATIONS * 2;
    private static final InetSocketAddress LOCALHOST = new InetSocketAddress("localhost", 6379);
    private final TransportFactory transportFactory = new SocketChannelTransportFactory(128);
    private final Driver driver = new Driver(transportFactory, 0);
    private ClientFacade[] clientFacades;

    @Before
    public void setUp() throws InterruptedException, ExecutionException, TimeoutException {
        driver.start();
        CompletionStage<ClientFacade> connection1 = driver.connect(LOCALHOST);
        CompletionStage<ClientFacade> connection2 = driver.connect(LOCALHOST);
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
            try {
                // TODO remove when driver has it's own threads
                Thread.sleep(1);
            } catch (InterruptedException ex) {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        // then
        for (int i = 0; i < WRITE_ITERATIONS; i++) {
            CompletionStage<String> actual = requests.get(i);
            assertThat(actual).isCompletedWithValue(generateString(i));
        }
    }

    private String generateString(int i) {
        return "x".repeat(i % 100) + "-" + i;
    }
}
