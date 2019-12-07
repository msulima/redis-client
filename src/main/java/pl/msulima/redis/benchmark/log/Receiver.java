package pl.msulima.redis.benchmark.log;

import org.agrona.concurrent.Agent;
import pl.msulima.redis.benchmark.log.network.RedisTransportPoller;
import pl.msulima.redis.benchmark.log.network.Transport;

class Receiver implements Agent {

    private final RedisTransportPoller redisTransportPoller;

    Receiver(RedisTransportPoller redisTransportPoller) {
        this.redisTransportPoller = redisTransportPoller;
    }

    public void registerReceiver(ReceiveChannelEndpoint receiveChannelEndpoint, Transport transport) {
        redisTransportPoller.registerForRead(receiveChannelEndpoint, transport);
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
