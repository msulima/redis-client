package pl.msulima.redis.benchmark.io;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.util.JedisClusterCRC16;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;

public class ClusterRouterConnection implements Connection {

    private static final int SLOTS_COUNT = 16384;

    private final Map<HostAndPort, Connection> hostToConnection;
    private final Map<Integer, HostAndPort> slotToHost;
    private final ConnectionFactory connectionFactory;

    public ClusterRouterConnection(String host, int port, ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        HostAndPort hostAndPort = new HostAndPort(host, port);
        Connection connection = this.connectionFactory.createConnection(hostAndPort);
        hostToConnection = new HashMap<>();
        hostToConnection.put(hostAndPort, connection);
        slotToHost = new HashMap<>();
        slotToHost.put(0, hostAndPort);
    }

    @Override
    public <T> void submit(Protocol.Command command, BiConsumer<T, Throwable> callback, byte[][] arguments) {
        submitToNode(getNode(arguments), command, callback, arguments);
    }

    private HostAndPort getNode(byte[][] arguments) {
        return slotToHost.getOrDefault(getSlot(arguments), slotToHost.get(0));
    }

    private int getSlot(byte[][] arguments) {
        if (arguments.length == 0) {
            return ThreadLocalRandom.current().nextInt(SLOTS_COUNT);
        } else {
            return JedisClusterCRC16.getCRC16(arguments[0]) & (SLOTS_COUNT - 1);
        }
    }

    private <T> void submitToNode(HostAndPort target, Protocol.Command command, BiConsumer<T, Throwable> callback, byte[][] arguments) {
        BiConsumer<T, Throwable> clusterAwareCallback = (result, error) -> {
            if (error instanceof JedisMovedDataException) {
                JedisMovedDataException jedisMovedDataException = (JedisMovedDataException) error;
                updateRoutingTable(jedisMovedDataException);
                submitToNode(jedisMovedDataException.getTargetNode(), command, callback, arguments);
            } else if (error instanceof JedisAskDataException) {
                JedisAskDataException jedisAskDataException = (JedisAskDataException) error;
                submitToNode(jedisAskDataException.getTargetNode(), command, callback, arguments);
            } else {
                callback.accept(result, error);
            }
        };

        getConnection(target).submit(command, clusterAwareCallback, arguments);
    }

    private void updateRoutingTable(JedisMovedDataException error) {
        slotToHost.put(error.getSlot(), error.getTargetNode());
    }

    private Connection getConnection(HostAndPort target) {
        return hostToConnection.computeIfAbsent(target, connectionFactory::createConnection);
    }

    @Override
    public void close() {
        hostToConnection.values().forEach(Connection::close);
    }
}
