package pl.msulima.redis.benchmark.log;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import pl.msulima.redis.benchmark.log.network.RedisTransportPoller;
import pl.msulima.redis.benchmark.log.network.Transport;

class Receiver implements Agent {

    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue = new OneToOneConcurrentArrayQueue<>(128);
    private final RedisTransportPoller redisTransportPoller;

    Receiver(RedisTransportPoller redisTransportPoller) {
        this.redisTransportPoller = redisTransportPoller;
    }

    public void registerChannelEndpoint(ReceiveChannelEndpoint receiveChannelEndpoint, Transport transport) {
        offer(() -> onRegisterChannelEndpoint(receiveChannelEndpoint, transport));
    }

    private void onRegisterChannelEndpoint(ReceiveChannelEndpoint receiveChannelEndpoint, Transport transport) {
        redisTransportPoller.registerForRead(receiveChannelEndpoint, transport);
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

    @Override
    public int doWork() {
        commandQueue.drain(Runnable::run);
        return redisTransportPoller.pollTransports();
    }

    @Override
    public void onClose() {

    }

    @Override
    public String roleName() {
        return "receiver";
    }
}
