package pl.msulima.redis.benchmark.nio;

import pl.msulima.redis.benchmark.jedis.PingOperation;

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
