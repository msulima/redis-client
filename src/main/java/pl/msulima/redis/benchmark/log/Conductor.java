package pl.msulima.redis.benchmark.log;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.logbuffer.PublicationImage;
import pl.msulima.redis.benchmark.log.presentation.ReceiverAgent;
import pl.msulima.redis.benchmark.log.presentation.SenderAgent;
import pl.msulima.redis.benchmark.log.protocol.DynamicEncoder;
import pl.msulima.redis.benchmark.log.session.NetworkAgent;
import pl.msulima.redis.benchmark.log.session.ReceiveChannelEndpoint;
import pl.msulima.redis.benchmark.log.session.SendChannelEndpoint;
import pl.msulima.redis.benchmark.log.transport.Transport;
import pl.msulima.redis.benchmark.log.transport.TransportFactory;
import pl.msulima.redis.benchmark.log.util.QueueUtil;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

class Conductor implements Agent {

    private final TransportFactory transportFactory;
    private final NetworkAgent networkAgent;
    private final int requestsQueueSize;
    private final int bufferSize;
    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue;
    private final SenderAgent senderAgent;
    private final ReceiverAgent receiverAgent;
    private final FinisherAgent finisherAgent;

    Conductor(FinisherAgent finisherAgent, NetworkAgent networkAgent, SenderAgent senderAgent, ReceiverAgent receiverAgent, TransportFactory transportFactory,
              int commandQueueSize, int requestsQueueSize, int bufferSize) {
        this.transportFactory = transportFactory;
        this.networkAgent = networkAgent;
        this.senderAgent = senderAgent;
        this.receiverAgent = receiverAgent;
        this.finisherAgent = finisherAgent;
        this.requestsQueueSize = requestsQueueSize;
        this.bufferSize = bufferSize;
        this.commandQueue = new OneToOneConcurrentArrayQueue<>(commandQueueSize);
    }

    CompletionStage<ClientFacade> connect(InetSocketAddress address) {
        CompletableFuture<ClientFacade> promise = new CompletableFuture<>();
        QueueUtil.offerOrSpin(commandQueue, () -> onConnect(promise, address));
        return promise;
    }

    private void onConnect(CompletableFuture<ClientFacade> promise, InetSocketAddress address) {
        ManyToOneConcurrentArrayQueue<Request<?>> requests = new ManyToOneConcurrentArrayQueue<>(requestsQueueSize);
        PublicationImage requestsImage = new PublicationImage(bufferSize * 2, DynamicEncoder.MAX_INTEGER_LENGTH);
        PublicationImage responsesImage = new PublicationImage(bufferSize * 2, DynamicEncoder.MAX_INTEGER_LENGTH);
        OneToOneConcurrentArrayQueue<Request<?>> responses = new OneToOneConcurrentArrayQueue<>(requestsQueueSize);
        OneToOneConcurrentArrayQueue<Request<?>> readyResponses = new OneToOneConcurrentArrayQueue<>(requestsQueueSize);

        SendChannelEndpoint sendChannelEndpoint = new SendChannelEndpoint(requestsImage);
        ReceiveChannelEndpoint receiveChannelEndpoint = new ReceiveChannelEndpoint(responsesImage);

        Transport transport = transportFactory.forAddress(address, bufferSize);
        networkAgent.registerChannelEndpoint(sendChannelEndpoint, receiveChannelEndpoint, transport);
        senderAgent.registerChannelEndpoint(requests, responses, requestsImage);
        receiverAgent.registerChannelEndpoint(responses, responsesImage, readyResponses);
        finisherAgent.registerChannelEndpoint(readyResponses);

        Connection connection = new Connection(requests);
        promise.complete(new ClientFacade(connection));
    }

    @Override
    public String roleName() {
        return "conductor";
    }

    @Override
    public int doWork() {
        return commandQueue.drain(Runnable::run);
    }

    @Override
    public void onClose() {
    }
}
