package pl.msulima.redis.benchmark.log;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.network.RedisTransportPoller;
import pl.msulima.redis.benchmark.log.network.Transport;
import pl.msulima.redis.benchmark.log.protocol.Response;
import redis.clients.jedis.Protocol;

import java.net.InetAddress;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

class Driver implements Agent {

    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue = new OneToOneConcurrentArrayQueue<>(128);

    private final Queue<Command<?>> requestQueue;
    private final Sender sender;
    private final Receiver receiver;
    private final TransportFactory transportFactory;

    Driver(int capacity, TransportFactory transportFactory) {
        this.requestQueue = new ManyToOneConcurrentArrayQueue<>(capacity);
        this.transportFactory = transportFactory;
        Queue<Command<?>> callbacksQueue = new OneToOneConcurrentArrayQueue<>(capacity);
        this.sender = new Sender(requestQueue, callbacksQueue);
        this.receiver = new Receiver(new RedisTransportPoller(1024));
    }

    public void connect(InetAddress address) {
        offer(() -> onConnect(address));
    }

    private void onConnect(InetAddress address) {
        Queue<Command<?>> callbacksQueue = new OneToOneConcurrentArrayQueue<>(1024);
        SendChannelEndpoint sendChannelEndpoint = new SendChannelEndpoint(callbacksQueue);
        ReceiveChannelEndpoint receiveChannelEndpoint = new ReceiveChannelEndpoint(callbacksQueue);
        Transport transport = transportFactory.forAddress(address);
        sender.registerChannelEndpoint(sendChannelEndpoint, transport);
        receiver.registerChannelEndpoint(receiveChannelEndpoint, transport);
    }

    <T> CompletionStage<T> offer(Protocol.Command cmd, Function<Response, T> deserializer, byte[]... args) {
        Command<T> command = new Command<>(cmd, deserializer, args);
        // TODO handle queue full
        requestQueue.offer(command);
        return command.getPromise();
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

    private void offer(Runnable runnable) {
        while (!commandQueue.offer(runnable)) {
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
            Thread.yield();
        }
        commandQueue.offer(runnable);
    }
}
