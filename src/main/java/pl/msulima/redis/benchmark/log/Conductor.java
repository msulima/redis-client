package pl.msulima.redis.benchmark.log;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.session.ReceiveChannelEndpoint;
import pl.msulima.redis.benchmark.log.session.SendChannelEndpoint;
import pl.msulima.redis.benchmark.log.transport.Transport;
import pl.msulima.redis.benchmark.log.transport.TransportFactory;
import pl.msulima.redis.benchmark.log.util.QueueUtil;

import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

class Conductor implements Agent {

    private static final int BUFFER_SIZE = 64 * 1024;
    private static final int REQUESTS_QUEUE_SIZE = 1024;

    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue;
    private final TransportFactory transportFactory;
    private final Sender sender;

    Conductor(TransportFactory transportFactory, Sender sender, int commandQueueSize) {
        this.transportFactory = transportFactory;
        this.sender = sender;
        this.commandQueue = new OneToOneConcurrentArrayQueue<>(commandQueueSize);
    }

    CompletionStage<ClientFacade> connect(InetSocketAddress address) {
        CompletableFuture<ClientFacade> promise = new CompletableFuture<>();
        QueueUtil.offerOrSpin(commandQueue, () -> onConnect(promise, address));
        return promise;
    }

    private void onConnect(CompletableFuture<ClientFacade> promise, InetSocketAddress address) {
        ManyToOneConcurrentArrayQueue<Request<?>> requestQueue = new ManyToOneConcurrentArrayQueue<>(REQUESTS_QUEUE_SIZE);
        Queue<Request<?>> callbacksQueue = new OneToOneConcurrentArrayQueue<>(REQUESTS_QUEUE_SIZE);
        Transport transport = transportFactory.forAddress(address);

        SendChannelEndpoint sendChannelEndpoint = new SendChannelEndpoint(requestQueue, callbacksQueue, BUFFER_SIZE);
        ReceiveChannelEndpoint receiveChannelEndpoint = new ReceiveChannelEndpoint(callbacksQueue, BUFFER_SIZE);
        sender.registerChannelEndpoint(sendChannelEndpoint, receiveChannelEndpoint, transport);

        Connection connection = new Connection(requestQueue);
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
