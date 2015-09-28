package pl.msulima.redis.benchmark.jedis;

import redis.clients.jedis.Pipeline;

public interface Operation {

    void run(Pipeline jedis);

    void done();
}
