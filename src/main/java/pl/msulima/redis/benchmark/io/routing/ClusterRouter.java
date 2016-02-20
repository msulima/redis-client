package pl.msulima.redis.benchmark.io.routing;

import pl.msulima.redis.benchmark.io.connection.Connection;
import pl.msulima.redis.benchmark.io.connection.ConnectionFactory;
import redis.clients.jedis.HostAndPort;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

public class ClusterRouter implements Closeable {

    private static final int SLOTS_COUNT = 16384;
    private final Map<HostAndPort, Connection> hostToConnection = new HashMap<>();
    private final Connection[] slotToConnection = new Connection[SLOTS_COUNT];

    public ClusterRouter(String host, int port, ConnectionFactory connectionFactory) {
        SlotToHost slotToHost = new SlotToHost(host, port);
        slotToHost.allHosts().forEach(h -> hostToConnection.put(h, connectionFactory.createConnection(h)));

        for (int slot = 0; slot < SLOTS_COUNT; slot++) {
            slotToConnection[slot] = hostToConnection.get(slotToHost.getHost(slot));
        }
    }

    public Connection getConnection(byte[][] arguments) {
        return slotToConnection[SlotToHost.getSlot(arguments)];
    }

    @Override
    public void close() {
        hostToConnection.values().forEach(Connection::close);
    }
}
