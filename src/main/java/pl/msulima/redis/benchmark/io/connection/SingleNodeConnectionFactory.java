package pl.msulima.redis.benchmark.io.connection;

import redis.clients.jedis.HostAndPort;

public class SingleNodeConnectionFactory implements ConnectionFactory {

    @Override
    public Connection createConnection(HostAndPort hostAndPort) {
        return new SingleNodeConnection(hostAndPort);
    }
}
