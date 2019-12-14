package pl.msulima.redis.benchmark.log;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.session.ReceiveChannelEndpoint;
import pl.msulima.redis.benchmark.log.session.RedisTransportPoller;
import pl.msulima.redis.benchmark.log.transport.Transport;
import pl.msulima.redis.benchmark.log.util.QueueUtil;

class Receiver implements Agent {

    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue;
    private final RedisTransportPoller redisTransportPoller;

    Receiver(RedisTransportPoller redisTransportPoller, int commandQueueSize) {
        this.redisTransportPoller = redisTransportPoller;
        this.commandQueue = new OneToOneConcurrentArrayQueue<>(commandQueueSize);
    }

    void registerChannelEndpoint(ReceiveChannelEndpoint receiveChannelEndpoint, Transport transport) {
        QueueUtil.offerOrSpin(commandQueue, () -> onRegisterChannelEndpoint(receiveChannelEndpoint, transport));
    }

    private void onRegisterChannelEndpoint(ReceiveChannelEndpoint receiveChannelEndpoint, Transport transport) {
        redisTransportPoller.registerForRead(receiveChannelEndpoint, transport);
    }

    @Override
    public String roleName() {
        return "receiver";
    }

    @Override
    public int doWork() {
        commandQueue.drain(Runnable::run);
        return redisTransportPoller.pollTransports();
    }

    @Override
    public void onClose() {
    }
}
