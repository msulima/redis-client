package pl.msulima.redis.benchmark.log;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.session.ReceiveChannelEndpoint;
import pl.msulima.redis.benchmark.log.session.RedisTransportPoller;
import pl.msulima.redis.benchmark.log.session.SendChannelEndpoint;
import pl.msulima.redis.benchmark.log.transport.Transport;
import pl.msulima.redis.benchmark.log.util.QueueUtil;

class NetworkAgent implements Agent {

    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue;
    private final RedisTransportPoller redisTransportPoller;

    NetworkAgent(RedisTransportPoller redisTransportPoller, int commandQueueSize) {
        this.redisTransportPoller = redisTransportPoller;
        this.commandQueue = new OneToOneConcurrentArrayQueue<>(commandQueueSize);
    }

    void registerChannelEndpoint(SendChannelEndpoint sendChannelEndpoint, ReceiveChannelEndpoint receiveChannelEndpoint, Transport transport) {
        QueueUtil.offerOrSpin(commandQueue, () -> onRegisterChannelEndpoint(sendChannelEndpoint, receiveChannelEndpoint, transport));
    }

    private void onRegisterChannelEndpoint(SendChannelEndpoint sendChannelEndpoint, ReceiveChannelEndpoint receiveChannelEndpoint, Transport transport) {
        redisTransportPoller.register(sendChannelEndpoint, receiveChannelEndpoint, transport);
    }

    @Override
    public String roleName() {
        return "networkAgent";
    }

    @Override
    public int doWork() {
        commandQueue.drain(Runnable::run);
        return redisTransportPoller.publishTransports();
    }

    @Override
    public void onClose() {
    }
}
