package pl.msulima.redis.benchmark.log;

import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.msulima.redis.benchmark.log.session.RedisTransportPublisher;
import pl.msulima.redis.benchmark.log.transport.CountedTransport;
import pl.msulima.redis.benchmark.log.transport.TransportFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

import static java.nio.charset.StandardCharsets.US_ASCII;

public class Driver implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Driver.class);

    private static final int COMMAND_QUEUE_SIZE = 512;
    private static final int COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT = 1024 * 1024;

    private final Conductor conductor;
    private final AgentRunner conductorRunner;
    private final AgentRunner networkRunner;

    public Driver(TransportFactory transportFactory, int useSelectorThreshold) {
        CountersManager countersManager = createCountersManager();
        AtomicCounter errorCounter = countersManager.newCounter("errorCounter");
        AtomicCounter sendSize = countersManager.newCounter("totalBytesSent");

        Sender sender = new Sender(new RedisTransportPublisher(useSelectorThreshold), COMMAND_QUEUE_SIZE);
        TransportFactory countedTransportFactory = address -> new CountedTransport(transportFactory.forAddress(address), sendSize);

        this.conductor = new Conductor(countedTransportFactory, sender, COMMAND_QUEUE_SIZE);
        this.networkRunner = new AgentRunner(new BackoffIdleStrategy(), this::errorHandler, errorCounter, sender);
        this.conductorRunner = new AgentRunner(new BackoffIdleStrategy(), this::errorHandler, errorCounter, conductor);
    }

    private static CountersManager createCountersManager() {
        UnsafeBuffer valuesBuffer = new UnsafeBuffer(new byte[COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT]);
        UnsafeBuffer metaDataBuffer = new UnsafeBuffer(new byte[countersMetadataBufferLength(COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT)]);
        return new CountersManager(metaDataBuffer, valuesBuffer, US_ASCII, () -> 0, 0);
    }

    public static int countersMetadataBufferLength(int counterValuesBufferLength) {
        return counterValuesBufferLength * (CountersReader.METADATA_LENGTH / CountersReader.COUNTER_LENGTH);
    }

    private void errorHandler(Throwable ex) {
        log.error("Error in driver", ex);
    }

    public void start() {
        AgentRunner.startOnThread(networkRunner);
        AgentRunner.startOnThread(conductorRunner);
    }

    public CompletionStage<ClientFacade> connect(InetSocketAddress address) {
        return conductor.connect(address);
    }

    @Override
    public void close() {
        CloseHelper.close(networkRunner);
        CloseHelper.close(conductorRunner);
    }
}
