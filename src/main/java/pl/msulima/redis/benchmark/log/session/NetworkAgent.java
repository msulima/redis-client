package pl.msulima.redis.benchmark.log.session;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.transport.Transport;
import pl.msulima.redis.benchmark.log.util.QueueUtil;

public class NetworkAgent implements Agent {

    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue;
    private final RedisTransportPoller redisTransportPoller;

    public NetworkAgent(RedisTransportPoller redisTransportPoller, int commandQueueSize) {
        this.redisTransportPoller = redisTransportPoller;
        this.commandQueue = new OneToOneConcurrentArrayQueue<>(commandQueueSize);
    }

    public void registerChannelEndpoint(SendChannelEndpoint sendChannelEndpoint, ReceiveChannelEndpoint receiveChannelEndpoint, Transport transport) {
        QueueUtil.offerOrSpin(commandQueue, () -> onRegisterChannelEndpoint(sendChannelEndpoint, receiveChannelEndpoint, transport));
    }

    private void onRegisterChannelEndpoint(SendChannelEndpoint sendChannelEndpoint, ReceiveChannelEndpoint receiveChannelEndpoint, Transport transport) {
        redisTransportPoller.register(sendChannelEndpoint, receiveChannelEndpoint, transport);
    }

    @Override
    public String roleName() {
        return "network";
    }

    @Override
    public int doWork() {
        commandQueue.drain(Runnable::run);
        return redisTransportPoller.publishTransports();
    }

    @Override
    public void onClose() {
        redisTransportPoller.close();
    }
}
