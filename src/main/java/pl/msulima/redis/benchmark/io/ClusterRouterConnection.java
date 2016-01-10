package pl.msulima.redis.benchmark.io;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisMovedDataException;

import java.util.function.BiConsumer;

public class ClusterRouterConnection implements Connection {

    private final ClusterRouter clusterRouter;

    public ClusterRouterConnection(String host, int port, ConnectionFactory connectionFactory) {
        this.clusterRouter = new ClusterRouter(host, port, connectionFactory);
    }

    @Override
    public <T> void submit(Protocol.Command command, BiConsumer<T, Throwable> callback, byte[][] arguments) {
        submitToNode(clusterRouter.getConnection(arguments), command, callback, arguments);
    }

    private <T> void submitToNode(Connection connection, Protocol.Command command, BiConsumer<T, Throwable> callback, byte[][] arguments) {
        BiConsumer<T, Throwable> clusterAwareCallback = (result, error) -> {
            if (error instanceof JedisMovedDataException) {
                JedisMovedDataException jedisMovedDataException = (JedisMovedDataException) error;
                clusterRouter.updateRoutingTable(jedisMovedDataException);
                submitToNode(clusterRouter.getConnection(jedisMovedDataException.getTargetNode()), command, callback, arguments);
            } else if (error instanceof JedisAskDataException) {
                JedisAskDataException jedisAskDataException = (JedisAskDataException) error;
                submitToNode(clusterRouter.getConnection(jedisAskDataException.getTargetNode()), command, callback, arguments);
            } else {
                callback.accept(result, error);
            }
        };

        connection.submit(command, clusterAwareCallback, arguments);
    }

    @Override
    public void close() {
        clusterRouter.close();
    }
}
