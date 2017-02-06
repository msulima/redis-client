package pl.msulima.redis.benchmark.nonblocking;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Queue;

public class Reader {

    private final ProtocolReader protocol;
    private Operation currentRead;
    private final Queue<Operation> readQueue;

    public Reader(Queue<Operation> readQueue, int bufferSize) {
        this.readQueue = readQueue;
        this.protocol = new ProtocolReader(bufferSize);
    }

    public void read(SocketChannel channel) throws IOException {
        if (protocol.receive(channel) == 0) {
            return;
        }

        Response response = new Response();

        if (currentRead != null && protocol.read(response)) {
            currentRead.finish(response);
            currentRead = null;
        }

        Operation task;
        while (currentRead == null && (task = readQueue.poll()) != null) {
            if (!protocol.read(response)) {
                currentRead = task;
            } else {
                task.finish(response);
            }
        }
    }
}
