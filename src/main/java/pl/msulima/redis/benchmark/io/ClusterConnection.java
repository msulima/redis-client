package pl.msulima.redis.benchmark.io;

import pl.msulima.redis.benchmark.io.routing.ClusterRouter;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisMovedDataException;

import java.util.function.BiConsumer;

public class ClusterConnection implements Connection {

    private final ClusterRouter clusterRouter;

    public ClusterConnection(String host, int port, ConnectionFactory connectionFactory) {
        this.clusterRouter = new ClusterRouter(host, port, connectionFactory);
    }

    @Override
    public <T> void submit(Protocol.Command command, BiConsumer<T, Throwable> callback, byte[][] arguments) {
        submitToNode(clusterRouter.getHostAndPort(arguments), command, callback, arguments);
    }

    private <T> void submitToNode(HostAndPort hostAndPort, Protocol.Command command, BiConsumer<T, Throwable> callback, byte[][] arguments) {
        BiConsumer<T, Throwable> clusterAwareCallback = (result, error) -> {
            if (error instanceof JedisMovedDataException) {
                JedisMovedDataException ex = (JedisMovedDataException) error;
                clusterRouter.updateRoutingTable(ex);
                submitToNode(ex.getTargetNode(), command, callback, arguments);
            } else if (error instanceof JedisAskDataException) {
                JedisAskDataException ex = (JedisAskDataException) error;
                submitToNode(ex.getTargetNode(), command, callback, arguments);
            } else if (error instanceof JedisConnectionException) {
                clusterRouter.markAsUnreachable(hostAndPort);
                submit(command, callback, arguments);
            } else {
                callback.accept(result, error);
            }
        };

        try {
            clusterRouter.getConnection(hostAndPort).submit(command, clusterAwareCallback, arguments);
        } catch (RuntimeException ex) {
            clusterRouter.markAsUnreachable(hostAndPort);
            callback.accept(null, ex);
        }
    }

    @Override
    public void close() {
        clusterRouter.close();
    }
}
