package pl.msulima.redis.benchmark.io;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class IoClient {

    private final static int CONNECTIONS = Integer.parseInt(System.getProperty("connections", "4"));
    private final List<IoConnection> connections;

    public static void main(String... args) {
        IoClient client = new IoClient();

        client.ping();
        for (int i = 0; i < 10; i++) {
            client.ping(Integer.toString(i));
        }
        client.ping();
    }

    public IoClient() {
        connections = new ArrayList<>(CONNECTIONS);
        for (int i = 0; i < CONNECTIONS; i++) {
            connections.add(new IoConnection());
        }
    }

    public CompletableFuture<byte[]> get(byte[] key) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        submit(new GetOperation(key, future::complete));
        return future;
    }

    public CompletableFuture<Long> del(byte[] key) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        submit(new DelOperation(key, future::complete));
        return future;
    }

    public CompletableFuture<Void> set(byte[] key, byte[] value) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        submit(new SetOperation(key, value, () -> future.complete(null)));
        return future;
    }

    public CompletableFuture<String> ping() {
        CompletableFuture<String> future = new CompletableFuture<>();
        submit(new PingOperation(future::complete));
        return future;
    }

    public CompletableFuture<String> ping(String text) {
        CompletableFuture<String> future = new CompletableFuture<>();
        submit(new PingOperation(text, future::complete));
        return future;
    }

    private void submit(Operation operation) {
        connections.get(operation.hashCode() % CONNECTIONS).submit(operation);
    }
}
