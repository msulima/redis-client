package pl.msulima.redis.benchmark.nonblocking;

import redis.clients.jedis.Protocol;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RedisStub implements Runnable {

    public static final int PORT = 6380;
    private final Map<ByteBuffer, byte[]> storage = new ConcurrentHashMap<>(8 * 1024);

    @Override
    public void run() {
        try {
            runInternal();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void runInternal() throws Exception {
        ExecutorService executorService = Executors.newCachedThreadPool();

        ServerSocket serverSocket = new ServerSocket(PORT);
        serverSocket.setReuseAddress(true);

        System.out.printf("S Server started at %d%n", PORT);
        AtomicInteger connectionIds = new AtomicInteger();

        while (!Thread.interrupted()) {
            Socket clientSocket = serverSocket.accept();
            int connectionId = connectionIds.incrementAndGet();

            executorService.submit(() -> {
                try {
                    handleConnection(clientSocket, connectionId);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            });
        }
        serverSocket.close();

        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
    }

    private void handleConnection(Socket clientSocket, int connectionId) throws IOException {
        log("New connection %d", connectionId);

        RedisInputStream is = new RedisInputStream(clientSocket.getInputStream());
        RedisOutputStream out = new RedisOutputStream(clientSocket.getOutputStream());

        while (!Thread.interrupted()) {
            @SuppressWarnings("unchecked") List<byte[]> query = (List<byte[]>) Protocol.read(is);

            String command = new String(query.get(0)).toLowerCase();

            switch (command) {
                case "get":
                    byte[] bytes = storage.get(ByteBuffer.wrap(query.get(1)));
                    out.write('$');
                    if (bytes == null) {
                        out.writeIntCrLf(-1);
                        out.writeCrLf();
                    } else {
                        out.writeIntCrLf(bytes.length);
                        out.write(bytes);
                        out.writeCrLf();
                    }
                    break;
                case "set":
                    storage.put(ByteBuffer.wrap(query.get(1)), query.get(2));
                    out.write("+OK\r\n".getBytes());
                    break;
                default:
                    throw new RuntimeException("Unknown command " + command);
            }

            out.flush();
        }

        log("Connection closed %d", connectionId);
        clientSocket.close();
    }

    private void log(String format, Object... args) {
        System.out.printf("[" + Thread.currentThread().getName() + "] " + format + "%n", args);
    }
}
