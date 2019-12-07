package pl.msulima.redis.benchmark.log;

import org.agrona.concurrent.Agent;
import pl.msulima.redis.benchmark.log.network.RedisTransportPoller;

import java.util.Queue;

class Receiver implements Agent {

    private final Queue<Command> callbackQueue;
    private final RedisTransportPoller redisTransportPoller;

    Receiver(Queue<Command> callbackQueue, RedisTransportPoller redisTransportPoller) {
        this.callbackQueue = callbackQueue;
        this.redisTransportPoller = redisTransportPoller;
    }

    @Override
    public int doWork() {
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
