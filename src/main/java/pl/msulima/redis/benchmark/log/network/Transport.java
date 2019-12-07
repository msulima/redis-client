package pl.msulima.redis.benchmark.log.network;

import java.nio.ByteBuffer;

interface Transport {

    void receive(ByteBuffer buffer);
}
