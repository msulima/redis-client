package pl.msulima.redis.benchmark.io;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.util.JedisClusterCRC16;

import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

public class ClusterRouter implements Closeable {

    private static final int SLOTS_COUNT = 16384;

    private final ConcurrentMap<HostAndPort, Connection> hostToConnection;
    private final ConcurrentMap<Integer, HostAndPort> slotToHost;
    private final ConnectionFactory connectionFactory;

    public ClusterRouter(String host, int port, ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        HostAndPort hostAndPort = new HostAndPort(host, port);
        Connection connection = this.connectionFactory.createConnection(hostAndPort);
        hostToConnection = new ConcurrentHashMap<>(SLOTS_COUNT);
        hostToConnection.put(hostAndPort, connection);
        slotToHost = new ConcurrentHashMap<>(SLOTS_COUNT);
        slotToHost.put(0, hostAndPort);
    }

    public void updateRoutingTable(JedisMovedDataException error) {
        slotToHost.put(error.getSlot(), error.getTargetNode());
    }

    public Connection getConnection(byte[][] arguments) {
        return hostToConnection.computeIfAbsent(getNode(arguments), connectionFactory::createConnection);
    }

    private HostAndPort getNode(byte[][] arguments) {
        return slotToHost.getOrDefault(getSlot(arguments), slotToHost.get(0));
    }

    public Connection getConnection(HostAndPort hostAndPort) {
        return hostToConnection.computeIfAbsent(hostAndPort, connectionFactory::createConnection);
    }

    private int getSlot(byte[][] arguments) {
        if (arguments.length == 0) {
            return ThreadLocalRandom.current().nextInt(SLOTS_COUNT);
        } else {
            return JedisClusterCRC16.getCRC16(arguments[0]) & (SLOTS_COUNT - 1);
        }
    }

    @Override
    public void close() {
        hostToConnection.values().forEach(Connection::close);
    }
}
