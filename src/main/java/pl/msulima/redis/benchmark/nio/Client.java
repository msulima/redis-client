package pl.msulima.redis.benchmark.nio;

import java.util.concurrent.CompletableFuture;

public class Client {

    private final Connection connection;

    public static void main(String... args) {
        Client client = new Client();

        client.ping();
        for (int i = 0; i < 10; i++) {
            client.ping(Integer.toString(i));
        }
        client.ping();
    }

    public Client() {
        connection = new Connection();
    }

    public CompletableFuture<byte[]> get(byte[] key) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        connection.submit(new GetOperation(key, future::complete));
        return future;
    }

    public CompletableFuture<Integer> del(byte[] key) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        connection.submit(new DelOperation(key, future::complete));
        return future;
    }

    public CompletableFuture<Void> set(byte[] key, byte[] value) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        connection.submit(new SetOperation(key, value, () -> future.complete(null)));
        return future;
    }

    public CompletableFuture<String> ping() {
        CompletableFuture<String> future = new CompletableFuture<>();
        connection.submit(new PingOperation(future::complete));
        return future;
    }

    public CompletableFuture<String> ping(String text) {
        CompletableFuture<String> future = new CompletableFuture<>();
        connection.submit(new PingOperation(text, future::complete));
        return future;
    }
}
