package pl.msulima.redis.benchmark.log.network;

import org.agrona.collections.ArrayUtil;
import pl.msulima.redis.benchmark.log.ReceiveChannelEndpoint;
import pl.msulima.redis.benchmark.log.protocol.DynamicDecoder;
import pl.msulima.redis.benchmark.log.protocol.Response;

import java.nio.ByteBuffer;

public class RedisTransportPoller {

    private final ByteBuffer buffer;
    private final DynamicDecoder decoder = new DynamicDecoder();
    private ChannelAndTransport[] channelAndTransports = new ChannelAndTransport[0];

    public RedisTransportPoller(int capacity) {
        this.buffer = ByteBuffer.allocateDirect(capacity);
    }

    public void registerForRead(ReceiveChannelEndpoint receiveChannelEndpoint, Transport transport) {
        this.channelAndTransports = ArrayUtil.add(channelAndTransports, new ChannelAndTransport(receiveChannelEndpoint, transport));
    }

    public int pollTransports() {
        int bytesRead = 0;
        for (ChannelAndTransport channelAndTransport : channelAndTransports) {
            bytesRead += pollTransport(channelAndTransport);
        }
        return bytesRead;
    }

    private int pollTransport(ChannelAndTransport channelAndTransport) {
        channelAndTransport.transport.receive(buffer);
        while (true) {
            decoder.read(buffer);
            Response response = decoder.response;
            if (response.isComplete()) {
                channelAndTransport.channelEndpoint.onResponse(response);
            } else {
                break;
            }
        }
        return buffer.position();
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
