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

    public void register(SendChannelEndpoint sendChannelEndpoint, ReceiveChannelEndpoint receiveChannelEndpoint, Transport transport) {
        ChannelAndTransport channelAndTransport = new ChannelAndTransport(sendChannelEndpoint, receiveChannelEndpoint, transport);
        transport.register(selector, channelAndTransport);
        this.channelAndTransports = ArrayUtil.add(channelAndTransports, channelAndTransport);
    }

    public int publishTransports() {
        if (channelAndTransports.length <= useSelectorThreshold) {
            int workDone = 0;
            for (ChannelAndTransport channelAndTransport : channelAndTransports) {
                workDone += channelAndTransport.sendChannel.send(channelAndTransport.transport);
                workDone += channelAndTransport.receiveChannel.receive(channelAndTransport.transport);
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
            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            workDone += 1;
        }
        if (key.isWritable()) {
            workDone += channelAndTransport.sendChannel.send(channelAndTransport.transport);
        }
        if (key.isReadable()) {
            workDone += channelAndTransport.receiveChannel.receive(channelAndTransport.transport);
        }
        return workDone;
    }

    @Override
    public void close() {
        super.close();
        for (ChannelAndTransport channelAndTransport : channelAndTransports) {
            channelAndTransport.transport.close();
        }
    }

    private static class ChannelAndTransport {
        private final SendChannelEndpoint sendChannel;
        private final ReceiveChannelEndpoint receiveChannel;
        private final Transport transport;

        private ChannelAndTransport(SendChannelEndpoint sendChannel, ReceiveChannelEndpoint receiveChannel, Transport transport) {
            this.receiveChannel = receiveChannel;
            this.transport = transport;
            this.sendChannel = sendChannel;
        }
    }
}
