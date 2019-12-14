package pl.msulima.redis.benchmark.log.transport;

import java.net.InetSocketAddress;

public interface TransportFactory {

    Transport forAddress(InetSocketAddress address);
}
