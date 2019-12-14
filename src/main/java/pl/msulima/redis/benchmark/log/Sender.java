package pl.msulima.redis.benchmark.log;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.session.RedisTransportPublisher;
import pl.msulima.redis.benchmark.log.session.SendChannelEndpoint;
import pl.msulima.redis.benchmark.log.transport.Transport;
import pl.msulima.redis.benchmark.log.util.QueueUtil;

class Sender implements Agent {

    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue;
    private final RedisTransportPublisher redisTransportPublisher;

    Sender(RedisTransportPublisher redisTransportPublisher, int commandQueueSize) {
        this.redisTransportPublisher = redisTransportPublisher;
        this.commandQueue = new OneToOneConcurrentArrayQueue<>(commandQueueSize);
    }

    void registerChannelEndpoint(SendChannelEndpoint sendChannelEndpoint, Transport transport) {
        QueueUtil.offerOrSpin(commandQueue, () -> onRegisterChannelEndpoint(sendChannelEndpoint, transport));
    }

    private void onRegisterChannelEndpoint(SendChannelEndpoint sendChannelEndpoint, Transport transport) {
        redisTransportPublisher.registerForWrite(sendChannelEndpoint, transport);
    }

    @Override
    public String roleName() {
        return "sender";
    }

    @Override
    public int doWork() {
        commandQueue.drain(Runnable::run);
        return redisTransportPublisher.publishTransports();
    }

    @Override
    public void onClose() {
    }
}
