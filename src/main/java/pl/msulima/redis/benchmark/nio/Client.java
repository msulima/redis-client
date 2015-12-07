package pl.msulima.redis.benchmark.nio;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Client {

    private final static int CONNECTIONS = Integer.parseInt(System.getProperty("connections", "4"));
    private final List<NioConnection> connections;

    public static void main(String... args) {
        Client client = new Client("localhost", 6379);

        client.ping();
        for (int i = 0; i < 10; i++) {
            client.ping(Integer.toString(i));
        }
        client.ping();
    }

    public Client(String host, int port) {
        connections = new ArrayList<>(CONNECTIONS);
        for (int i = 0; i < CONNECTIONS; i++) {
            connections.add(new NioConnection(host, port));
        }
    }

    public CompletableFuture<byte[]> get(byte[] key) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        submit(new GetOperation(key, future::complete));
        return future;
    }

    public CompletableFuture<Integer> del(byte[] key) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
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
