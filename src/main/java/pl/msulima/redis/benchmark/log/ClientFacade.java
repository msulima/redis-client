package pl.msulima.redis.benchmark.log;

import pl.msulima.redis.benchmark.log.protocol.Command;
import pl.msulima.redis.benchmark.log.protocol.DynamicEncoder;

import java.util.concurrent.CompletionStage;

public class ClientFacade {

    private final Connection connection;

    public ClientFacade(Connection connection) {
        this.connection = connection;
    }

    public CompletionStage<String> ping() {
        return connection.offer(Command.PING, Request::getSimpleString);
    }

    public CompletionStage<String> ping(String text) {
        return connection.offer(Command.PING, Request::decodeBulkString, text.getBytes(DynamicEncoder.CHARSET));
    }

    public CompletionStage<String> set(byte[] key, byte[] value) {
        return connection.offer(Command.SET, Request::getSimpleString, key, value);
    }

    public CompletionStage<byte[]> get(byte[] key) {
        return connection.offer(Command.GET, Request::getBulkString, key);
    }
}
