package pl.msulima.redis.benchmark.io;

import pl.msulima.redis.benchmark.io.connection.ClusterConnection;
import pl.msulima.redis.benchmark.io.connection.SingleNodeConnectionFactory;
import redis.clients.jedis.Protocol;
import redis.clients.util.SafeEncoder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

public class SyncClient {

    private final ClusterConnection connection;

    public static void main(String... args) {
        SyncClient client = new SyncClient("127.0.0.1", 30001);

        client.ping();
        for (int i = 0; i < 10; i++) {
            client.get(Integer.toString(i).getBytes());
        }
        client.ping();
    }

    public SyncClient(String host, int port) {
        connection = new ClusterConnection(host, port, new SingleNodeConnectionFactory());
    }

    public CompletionStage<byte[]> get(byte[] key) {
        return createAndSubmit(Protocol.Command.GET, key);
    }

    public void get(byte[] key, BiConsumer<byte[], Throwable> callback) {
        submit(Protocol.Command.GET, callback, key);
    }

    public CompletionStage<Long> del(byte[] key) {
        return createAndSubmit(Protocol.Command.DEL, key);
    }

    public CompletionStage<String> set(byte[] key, byte[] value) {
        return createAndSubmitWithStatusCode(Protocol.Command.SET, key, value);
    }

    public void set(byte[] key, byte[] value, BiConsumer<byte[], Throwable> callback) {
        submit(Protocol.Command.SET, callback, key, value);
    }

    public CompletionStage<String> setex(byte[] key, int ttl, byte[] value) {
        return createAndSubmitWithStatusCode(Protocol.Command.SETEX, key, Protocol.toByteArray(ttl), value);
    }

    public CompletionStage<String> ping() {
        CompletionStage<byte[]> future = createAndSubmit(Protocol.Command.PING);
        return future.thenApply(SafeEncoder::encode);
    }

    public CompletionStage<String> ping(String text) {
        CompletionStage<byte[]> future = createAndSubmit(Protocol.Command.PING, SafeEncoder.encode(text));
        return future.thenApply(SafeEncoder::encode);
    }

    private CompletionStage<String> createAndSubmitWithStatusCode(Protocol.Command set, byte[]... arguments) {
        CompletionStage<byte[]> response = createAndSubmit(set, arguments);
        return response.thenApply(SafeEncoder::encode);
    }

    private <T> CompletionStage<T> createAndSubmit(Protocol.Command command, byte[]... arguments) {
        CompletableFuture<T> future = new CompletableFuture<>();
        submit(command, (result, error) -> {
            if (error == null) {
                future.complete((T) result);
            } else {
                future.completeExceptionally(error);
            }
        }, arguments);
        return future;
    }

    private <T> void submit(Protocol.Command command, BiConsumer<T, Throwable> callback, byte[]... arguments) {
        connection.submit(command, callback, arguments);
    }
}
