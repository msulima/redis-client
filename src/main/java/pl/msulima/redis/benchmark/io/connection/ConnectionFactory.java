package pl.msulima.redis.benchmark.io.connection;


import redis.clients.jedis.HostAndPort;

public interface ConnectionFactory {

    Connection createConnection(HostAndPort hostAndPort);
}
