package pl.msulima.redis.benchmark.io;

import redis.clients.jedis.Protocol;
import redis.clients.util.SafeEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class IoClient {

    private final List<IoConnection> connections;

    public static void main(String... args) {
        IoClient client = new IoClient("127.0.0.1", 6379, 4);

        client.ping();
        for (int i = 0; i < 10; i++) {
            client.ping(Integer.toString(i));
        }
        client.ping();
    }

    public IoClient(String host, int port, int connectionsCount) {
        connections = new ArrayList<>(connectionsCount);
        for (int i = 0; i < connectionsCount; i++) {
            connections.add(new IoConnection(host, port));
        }
    }

    public CompletableFuture<byte[]> get(byte[] key) {
        return createAndSubmit(Protocol.Command.GET, key);
    }

    public CompletableFuture<Long> del(byte[] key) {
        return createAndSubmit(Protocol.Command.DEL, key);
    }

    public CompletableFuture<String> set(byte[] key, byte[] value) {
        return createAndSubmitWithStatusCode(Protocol.Command.SET, key, value);
    }

    public CompletableFuture<String> setex(byte[] key, int ttl, byte[] value) {
        return createAndSubmitWithStatusCode(Protocol.Command.SETEX, key, Protocol.toByteArray(ttl), value);
    }

    public CompletableFuture<String> ping() {
        CompletableFuture<byte[]> future = createAndSubmit(Protocol.Command.PING);
        return future.thenApply(SafeEncoder::encode);
    }

    public CompletableFuture<String> ping(String text) {
        CompletableFuture<byte[]> future = createAndSubmit(Protocol.Command.PING, SafeEncoder.encode(text));
        return future.thenApply(SafeEncoder::encode);
    }

    private CompletableFuture<String> createAndSubmitWithStatusCode(Protocol.Command set, byte[]... arguments) {
        CompletableFuture<byte[]> response = createAndSubmit(set, arguments);
        return response.thenApply(SafeEncoder::encode);
    }

    private <T> CompletableFuture<T> createAndSubmit(Protocol.Command command, byte[]... arguments) {
        CompletableFuture<T> future = new CompletableFuture<>();
        submit(new ResultCommand<>(command, future, arguments));
        return future;
    }

    private void submit(Command command) {
        connections.get(command.hashCode() % connections.size()).submit(command);
    }
}
