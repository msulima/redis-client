package pl.msulima.redis.benchmark.log.session;

import org.agrona.LangUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.nio.TransportPoller;
import pl.msulima.redis.benchmark.log.transport.Transport;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public class RedisTransportPublisher extends TransportPoller {

    private ChannelAndTransport[] channelAndTransports = new ChannelAndTransport[0];
    private final int useSelectorThreshold;

    public RedisTransportPublisher(int useSelectorThreshold) {
        this.useSelectorThreshold = useSelectorThreshold;
    }

    public void registerForWrite(SendChannelEndpoint receiveChannelEndpoint, Transport transport) {
        ChannelAndTransport channelAndTransport = new ChannelAndTransport(receiveChannelEndpoint, transport);
        transport.register(selector, channelAndTransport, SelectionKey.OP_WRITE);
        this.channelAndTransports = ArrayUtil.add(channelAndTransports, channelAndTransport);
    }

    public int publishTransports() {
        if (channelAndTransports.length <= useSelectorThreshold) {
            int workDone = 0;
            for (ChannelAndTransport channelAndTransport : channelAndTransports) {
                workDone += channelAndTransport.channelEndpoint.send(channelAndTransport.transport);
            }
            return workDone;
        }

        try {
            selector.selectNow();
        } catch (IOException ex) {
            LangUtil.rethrowUnchecked(ex);
        }
        return selectedKeySet.forEach(key -> {
            return publishTransport(key, (ChannelAndTransport) key.attachment());
        });
    }

    private int publishTransport(SelectionKey key, ChannelAndTransport channelAndTransport) {
        int workDone = 0;
        if (key.isConnectable()) {
            channelAndTransport.transport.connect();
            key.interestOps(SelectionKey.OP_WRITE);
            workDone += 1;
        }
        if (key.isWritable()) {
            workDone += channelAndTransport.channelEndpoint.send(channelAndTransport.transport);
        }
        return workDone;
    }

    private static class ChannelAndTransport {
        private final SendChannelEndpoint channelEndpoint;
        private final Transport transport;

        private ChannelAndTransport(SendChannelEndpoint channelEndpoint, Transport transport) {
            this.transport = transport;
            this.channelEndpoint = channelEndpoint;
        }
    }
}
