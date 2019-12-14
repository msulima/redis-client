package pl.msulima.redis.benchmark.log;

import pl.msulima.redis.benchmark.log.protocol.DynamicEncoder;
import redis.clients.jedis.Protocol;

import java.util.concurrent.CompletionStage;

public class ClientFacade {

    private final Connection connection;

    public ClientFacade(Connection connection) {
        this.connection = connection;
    }

    public CompletionStage<String> ping() {
        return connection.offer(Protocol.Command.PING, Request::getSimpleString);
    }

    public CompletionStage<String> ping(String text) {
        return connection.offer(Protocol.Command.PING, Request::decodeBulkString, text.getBytes(DynamicEncoder.CHARSET));
    }

    public CompletionStage<String> set(byte[] key, byte[] value) {
        return connection.offer(Protocol.Command.SET, Request::getSimpleString, key, value);
    }

    public CompletionStage<byte[]> get(byte[] key) {
        return connection.offer(Protocol.Command.GET, Request::getBulkString, key);
    }
}
