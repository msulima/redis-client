package pl.msulima.redis.benchmark.io;

import redis.clients.jedis.Protocol;
import redis.clients.util.SafeEncoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

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

    public void get(byte[] key, Consumer<byte[]> callback, Consumer<Throwable> onError) {
        createAndSubmit2(Protocol.Command.GET, callback, onError, key);
    }

    public CompletableFuture<Long> del(byte[] key) {
        return createAndSubmit(Protocol.Command.DEL, key);
    }

    public CompletableFuture<String> set(byte[] key, byte[] value) {
        return createAndSubmitWithStatusCode(Protocol.Command.SET, key, value);
    }

    public void set(byte[] key, byte[] value, Consumer<byte[]> callback, Consumer<Throwable> onError) {
        createAndSubmit2(Protocol.Command.SET, callback, onError, key, value);
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
        createAndSubmit2(command, future::complete, future::completeExceptionally, arguments);
        return future;
    }

    private <T> void createAndSubmit2(Protocol.Command command, Consumer<T> callback, Consumer<Throwable> onError, byte[]... arguments) {
        submit(command, callback, onError, arguments);
    }

    private <T> void submit(Protocol.Command command, Consumer<T> callback, Consumer<Throwable> onError, byte[][] arguments) {
        CommandHolder<T> commandHolder = new CommandHolder<>(command, callback, onError, arguments);

        if (connections.size() > 1 && arguments.length > 0) {
            connections.get(Math.abs(Arrays.hashCode(arguments[0])) % connections.size()).submit(commandHolder);
        } else {
            connections.get(0).submit(commandHolder);
        }
    }
}
