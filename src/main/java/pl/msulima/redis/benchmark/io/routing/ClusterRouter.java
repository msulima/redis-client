package pl.msulima.redis.benchmark.io.routing;

import pl.msulima.redis.benchmark.WithLock;
import pl.msulima.redis.benchmark.io.Connection;
import pl.msulima.redis.benchmark.io.ConnectionFactory;
import pl.msulima.redis.benchmark.io.UnreachableConnection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.exceptions.JedisMovedDataException;

import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ClusterRouter implements Closeable {

    private static final int GRACE_PERIOD_MILLIS = 2000;

    private final ConcurrentMap<HostAndPort, Connection> hostToConnection;
    private final ConnectionFactory connectionFactory;
    private final SlotToHost slotToHost;
    private final WithLock lock = new WithLock();

    public ClusterRouter(String host, int port, ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        HostAndPort hostAndPort = new HostAndPort(host, port);
        Connection connection = this.connectionFactory.createConnection(hostAndPort);
        hostToConnection = new ConcurrentHashMap<>();
        hostToConnection.put(hostAndPort, connection);
        slotToHost = new SlotToHost(hostAndPort);
    }

    public void updateRoutingTable(JedisMovedDataException error) {
        slotToHost.updateRoutingTable(error);
    }

    public void markAsUnreachable(HostAndPort hostAndPort) {
        lock.writing(() -> {
            System.out.println("Shutting down " + hostAndPort);
            slotToHost.markAsUnreachable(hostAndPort, hostToConnection.keySet().iterator().next());

            Connection connection = hostToConnection.remove(hostAndPort);
            if (connection != null) {
                connection.close();
            }
            return null;
        });
    }

    public HostAndPort getHostAndPort(byte[][] arguments) {
        return slotToHost.getHostAndPort(arguments);
    }

    public Connection getConnection(HostAndPort hostAndPort) {
        Connection connection = lock.reading(() -> hostToConnection.get(hostAndPort));

        if (connection == null) {
            return lock.writing(() -> hostToConnection.computeIfAbsent(hostAndPort, (h) -> {
                try {
                    return connectionFactory.createConnection(hostAndPort);
                } catch (RuntimeException ex) {
                    return new UnreachableConnection(hostAndPort, GRACE_PERIOD_MILLIS);
                }
            }));
        }

        return connection;
    }

    @Override
    public void close() {
        lock.writing(() -> {
            hostToConnection.values().forEach(Connection::close);
            return null;
        });
    }
}
