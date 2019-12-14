package pl.msulima.redis.benchmark.log;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.presentation.ReceiverAgent;
import pl.msulima.redis.benchmark.log.presentation.SenderAgent;
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

    Conductor(TransportFactory transportFactory, NetworkAgent networkAgent, SenderAgent senderAgent, ReceiverAgent receiverAgent, int commandQueueSize, int requestsQueueSize, int bufferSize) {
        this.transportFactory = transportFactory;
        this.networkAgent = networkAgent;
        this.senderAgent = senderAgent;
        this.receiverAgent = receiverAgent;
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
        OneToOneConcurrentArrayQueue<byte[]> requestsBinary = new OneToOneConcurrentArrayQueue<>(requestsQueueSize);
        OneToOneConcurrentArrayQueue<byte[]> responsesBinary = new OneToOneConcurrentArrayQueue<>(requestsQueueSize);
        OneToOneConcurrentArrayQueue<Request<?>> responses = new OneToOneConcurrentArrayQueue<>(requestsQueueSize);

        SendChannelEndpoint sendChannelEndpoint = new SendChannelEndpoint(requestsBinary);
        ReceiveChannelEndpoint receiveChannelEndpoint = new ReceiveChannelEndpoint(bufferSize, responsesBinary);

        Transport transport = transportFactory.forAddress(address, bufferSize);
        networkAgent.registerChannelEndpoint(sendChannelEndpoint, receiveChannelEndpoint, transport);
        senderAgent.registerChannelEndpoint(requests, requestsBinary, responses);
        receiverAgent.registerChannelEndpoint(responsesBinary, responses);

        Connection connection = new Connection(requests);
        promise.complete(new ClientFacade(connection));
    }

    @Override
    public String roleName() {
        return "agent";
    }

    @Override
    public int doWork() {
        return commandQueue.drain(Runnable::run);
    }

    @Override
    public void onClose() {
    }
}
