package pl.msulima.redis.benchmark.io.routing;

import pl.msulima.redis.benchmark.WithLock;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.util.JedisClusterCRC16;

import java.util.concurrent.ThreadLocalRandom;

public class SlotToHost {

    private static final int SLOTS_COUNT = 16384;

    private final HostAndPort[] slotToHost = new HostAndPort[SLOTS_COUNT];
    private final WithLock lock = new WithLock();

    public SlotToHost(HostAndPort defaultPort) {
        for (int i = 0; i < SLOTS_COUNT; i++) {
            slotToHost[i] = defaultPort;
        }
    }

    public void updateRoutingTable(JedisMovedDataException error) {
        lock.writing(() -> slotToHost[error.getSlot()] = error.getTargetNode());
    }

    public void markAsUnreachable(HostAndPort hostAndPort, HostAndPort defaultTarget) {
        lock.writing(() -> {
            for (int i = 0; i < SLOTS_COUNT; i++) {
                if (hostAndPort.equals(slotToHost[i])) {
                    slotToHost[i] = defaultTarget;
                }
            }
            return null;
        });
    }

    public HostAndPort getHostAndPort(byte[][] arguments) {
        int slot = getSlot(arguments);

        return lock.reading(() -> slotToHost[slot]);
    }

    private static int getSlot(byte[][] arguments) {
        if (arguments.length == 0) {
            return ThreadLocalRandom.current().nextInt(SLOTS_COUNT);
        } else {
            return JedisClusterCRC16.getCRC16(arguments[0]) & (SLOTS_COUNT - 1);
        }
    }
}
