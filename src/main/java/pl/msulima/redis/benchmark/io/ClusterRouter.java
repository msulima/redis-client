package pl.msulima.redis.benchmark.io;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.util.JedisClusterCRC16;

import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class ClusterRouter implements Closeable {

    private static final int SLOTS_COUNT = 16384;
    private static final int GRACE_PERIOD_MILLIS = 2000;

    private final ConcurrentMap<HostAndPort, Connection> hostToConnection;
    private final ConcurrentMap<Integer, HostAndPort> slotToHost;
    private final ConnectionFactory connectionFactory;
    private final ReentrantReadWriteLock lock;

    public ClusterRouter(String host, int port, ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        HostAndPort hostAndPort = new HostAndPort(host, port);
        Connection connection = this.connectionFactory.createConnection(hostAndPort);
        hostToConnection = new ConcurrentHashMap<>(SLOTS_COUNT);
        hostToConnection.put(hostAndPort, connection);
        slotToHost = new ConcurrentHashMap<>(SLOTS_COUNT);
        slotToHost.put(0, hostAndPort);
        lock = new ReentrantReadWriteLock();
    }

    public void updateRoutingTable(JedisMovedDataException error) {
        writing(() -> slotToHost.put(error.getSlot(), error.getTargetNode()));
    }

    public void markAsUnreachable(HostAndPort hostAndPort) {
        writing(() -> {
            System.out.println("Shutting down " + hostAndPort);
            slotToHost.entrySet().removeIf((entry) -> entry.getValue().equals(hostAndPort));

            Connection connection = hostToConnection.remove(hostAndPort);
            if (connection != null) {
                connection.close();
            }
            return null;
        });
    }

    public HostAndPort getHostAndPort(byte[][] arguments) {
        int slot = getSlot(arguments);

        return reading(() -> slotToHost.getOrDefault(slot, slotToHost.entrySet().iterator().next().getValue()));
    }

    private static int getSlot(byte[][] arguments) {
        if (arguments.length == 0) {
            return ThreadLocalRandom.current().nextInt(SLOTS_COUNT);
        } else {
            return JedisClusterCRC16.getCRC16(arguments[0]) & (SLOTS_COUNT - 1);
        }
    }

    public Connection getConnection(HostAndPort hostAndPort) {
        Connection connection = reading(() -> hostToConnection.get(hostAndPort));

        if (connection == null) {
            return writing(() -> hostToConnection.computeIfAbsent(hostAndPort, (h) -> {
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
        writing(() -> {
            hostToConnection.values().forEach(Connection::close);
            return null;
        });
    }

    private <T> T reading(Supplier<T> function) {
        try {
            lock.readLock().lock();
            return function.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    private <T> T writing(Supplier<T> function) {
        try {
            lock.writeLock().lock();
            return function.get();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
