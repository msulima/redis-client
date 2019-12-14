package pl.msulima.redis.benchmark.log.session;

import org.agrona.LangUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.nio.TransportPoller;
import pl.msulima.redis.benchmark.log.transport.Transport;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public class RedisTransportPoller extends TransportPoller {

    private ChannelAndTransport[] channelAndTransports = new ChannelAndTransport[0];
    private final int useSelectorThreshold;

    public RedisTransportPoller(int useSelectorThreshold) {
        this.useSelectorThreshold = useSelectorThreshold;
    }

    public void registerForRead(ReceiveChannelEndpoint receiveChannelEndpoint, Transport transport) {
        ChannelAndTransport channelAndTransport = new ChannelAndTransport(receiveChannelEndpoint, transport);
        transport.register(selector, channelAndTransport, SelectionKey.OP_READ);
        this.channelAndTransports = ArrayUtil.add(channelAndTransports, channelAndTransport);
    }

    public int pollTransports() {
        if (channelAndTransports.length <= useSelectorThreshold) {
            int workDone = 0;
            for (ChannelAndTransport channelAndTransport : channelAndTransports) {
                workDone += channelAndTransport.channelEndpoint.receive(channelAndTransport.transport);
            }
            return workDone;
        }

        try {
            selector.selectNow();
        } catch (IOException ex) {
            LangUtil.rethrowUnchecked(ex);
        }
        return selectedKeySet.forEach(key -> {
            return pollTransport(key, (ChannelAndTransport) key.attachment());
        });
    }

    private int pollTransport(SelectionKey key, ChannelAndTransport channelAndTransport) {
        int workDone = 0;
        if (key.isConnectable()) {
            channelAndTransport.transport.connect();
            key.interestOps(SelectionKey.OP_READ);
            workDone += 1;
        }
        if (key.isReadable()) {
            workDone += channelAndTransport.channelEndpoint.receive(channelAndTransport.transport);
        }
        return workDone;
    }

    private static class ChannelAndTransport {
        private final ReceiveChannelEndpoint channelEndpoint;
        private final Transport transport;

        private ChannelAndTransport(ReceiveChannelEndpoint channelEndpoint, Transport transport) {
            this.transport = transport;
            this.channelEndpoint = channelEndpoint;
        }
    }
}
