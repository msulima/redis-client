package pl.msulima.redis.benchmark.io.connection;

import pl.msulima.redis.benchmark.io.routing.ClusterRouter;
import redis.clients.jedis.Protocol;

import java.util.function.BiConsumer;

public class ClusterConnection implements Connection {

    private final ClusterRouter clusterRouter;

    public ClusterConnection(String host, int port, ConnectionFactory connectionFactory) {
        this.clusterRouter = new ClusterRouter(host, port, connectionFactory);
    }

    @Override
    public <T> void submit(Protocol.Command command, BiConsumer<T, Throwable> callback, byte[][] arguments) {
        SingleNodeConnection connection = clusterRouter.getConnection(arguments);
        connection.submit(command, callback, arguments);
    }

    @Override
    public void close() {
        clusterRouter.close();
    }
}
