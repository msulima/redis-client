package pl.msulima.redis.benchmark.log.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;

public class SocketChannelTransportFactory implements TransportFactory {

    @Override
    public Transport forAddress(InetSocketAddress address, int bufferSize) {
        SocketChannel channel = createChannel(address, bufferSize);
        return new SocketChannelTransport(channel);
    }

    private SocketChannel createChannel(InetSocketAddress serverAddress, int bufferSize) {
        try {
            SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            channel.setOption(StandardSocketOptions.SO_RCVBUF, bufferSize);
            channel.setOption(StandardSocketOptions.SO_SNDBUF, bufferSize);
            channel.connect(serverAddress);

            return channel;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
