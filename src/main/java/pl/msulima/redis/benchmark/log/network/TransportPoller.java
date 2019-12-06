package pl.msulima.redis.benchmark.log.network;

import pl.msulima.redis.benchmark.log.ReceiveChannelEndpoint;
import pl.msulima.redis.benchmark.log.protocol.DynamicDecoder;

import java.nio.ByteBuffer;

class TransportPoller {

    private final ByteBuffer buffer;
    private final DynamicDecoder decoder = new DynamicDecoder();

    public TransportPoller(int capacity) {
        buffer = ByteBuffer.allocateDirect(capacity);
    }

    void pollTransports() {
    }

    void pollTransport(ChannelAndTransport channelAndTransport) {
        channelAndTransport.transport.receive(buffer);
        decoder.read(buffer);
        if (decoder.response.isComplete()) {
            channelAndTransport.channelEndpoint.onResponse(decoder.response);
        }
    }

    private static class ChannelAndTransport {
        private final Transport transport;
        private final ReceiveChannelEndpoint channelEndpoint;

        public ChannelAndTransport(Transport transport, ReceiveChannelEndpoint channelEndpoint) {
            this.transport = transport;
            this.channelEndpoint = channelEndpoint;
        }
    }
}
