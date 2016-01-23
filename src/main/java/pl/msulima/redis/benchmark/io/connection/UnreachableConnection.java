package pl.msulima.redis.benchmark.io.connection;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Protocol;

import java.util.function.BiConsumer;

public class UnreachableConnection implements Connection {

    private final int gracePeriodMillis;
    private final HostAndPort hostAndPort;
    private long start;

    public UnreachableConnection(HostAndPort hostAndPort, int gracePeriodMillis) {
        this.gracePeriodMillis = gracePeriodMillis;
        this.hostAndPort = hostAndPort;
        start = System.currentTimeMillis();
    }

    @Override
    public <T> void submit(Protocol.Command command, BiConsumer<T, Throwable> callback, byte[][] arguments) {
        long timePassed = System.currentTimeMillis() - start;
        long gracePeriodLeft = gracePeriodMillis - timePassed;
        if (gracePeriodLeft < 0) {
            throw new RuntimeException("Connection to " + this.hostAndPort + " unreachable, grace period ended.");
        }
        callback.accept(null, new RuntimeException("Connection to " + this.hostAndPort +
                " unreachable, grace period ends in " + gracePeriodLeft + " ms"));
    }

    @Override
    public void close() {
        start = 0;
    }
}
