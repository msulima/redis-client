package pl.msulima.redis.benchmark.io;


import redis.clients.jedis.HostAndPort;

public interface ConnectionFactory {

    Connection createConnection(HostAndPort hostAndPort);
}
