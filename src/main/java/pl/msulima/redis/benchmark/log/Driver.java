package pl.msulima.redis.benchmark.log;

import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.CompositeAgent;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.msulima.redis.benchmark.log.presentation.ReceiverAgent;
import pl.msulima.redis.benchmark.log.presentation.SenderAgent;
import pl.msulima.redis.benchmark.log.session.NetworkAgent;
import pl.msulima.redis.benchmark.log.session.RedisTransportPoller;
import pl.msulima.redis.benchmark.log.transport.CountedTransport;
import pl.msulima.redis.benchmark.log.transport.TransportFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

import static java.nio.charset.StandardCharsets.US_ASCII;

public class Driver implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Driver.class);

    private static final int COMMAND_QUEUE_SIZE = 128;
    private static final int REQUESTS_QUEUE_SIZE = 64 * 1024;
    private static final int COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT = 1024 * 1024;
    private static final int BUFFER_SIZE = 8 * 1024 * 1024;

    private final Conductor conductor;
    private final AgentRunner networkRunner;
    private final AgentRunner senderRunner;
    private final AgentRunner receiverRunner;
    private final AgentRunner finisherRunner;

    public Driver(TransportFactory transportFactory, int useSelectorThreshold) {
        CountersManager countersManager = createCountersManager();
        AtomicCounter errorCounter = countersManager.newCounter("errorCounter");
        AtomicCounter sendSize = countersManager.newCounter("totalBytesSent");

        FinisherAgent finisherAgent = new FinisherAgent(COMMAND_QUEUE_SIZE);
        NetworkAgent networkAgent = new NetworkAgent(new RedisTransportPoller(useSelectorThreshold), COMMAND_QUEUE_SIZE);
        ReceiverAgent receiverAgent = new ReceiverAgent(COMMAND_QUEUE_SIZE);
        SenderAgent senderAgent = new SenderAgent(COMMAND_QUEUE_SIZE);

        TransportFactory countedTransportFactory = (address, bufferSize) -> new CountedTransport(transportFactory.forAddress(address, bufferSize), sendSize);
        this.conductor = new Conductor(finisherAgent, networkAgent, senderAgent, receiverAgent, countedTransportFactory, COMMAND_QUEUE_SIZE, REQUESTS_QUEUE_SIZE, BUFFER_SIZE);

        this.finisherRunner = new AgentRunner(new BackoffIdleStrategy(), this::errorHandler, errorCounter, finisherAgent);
        this.networkRunner = new AgentRunner(new BackoffIdleStrategy(), this::errorHandler, errorCounter, new CompositeAgent(conductor, networkAgent));
        this.receiverRunner = new AgentRunner(new BackoffIdleStrategy(), this::errorHandler, errorCounter, receiverAgent);
        this.senderRunner = new AgentRunner(new BackoffIdleStrategy(), this::errorHandler, errorCounter, senderAgent);
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
        AgentRunner.startOnThread(finisherRunner);
        AgentRunner.startOnThread(networkRunner);
        AgentRunner.startOnThread(receiverRunner);
        AgentRunner.startOnThread(senderRunner);
    }

    public CompletionStage<ClientFacade> connect(InetSocketAddress address) {
        return conductor.connect(address);
    }

    @Override
    public void close() {
        CloseHelper.close(finisherRunner);
        CloseHelper.close(networkRunner);
        CloseHelper.close(receiverRunner);
        CloseHelper.close(senderRunner);
    }
}
