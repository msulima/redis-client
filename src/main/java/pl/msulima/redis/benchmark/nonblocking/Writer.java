package pl.msulima.redis.benchmark.nonblocking;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.Queue;

public class Writer {

    private final Queue<Operation> writeQueue;
    private final Queue<Operation> readQueue;
    private final ProtocolWriter protocol;

    private Operation currentWrite;

    public Writer(Queue<Operation> writeQueue, Queue<Operation> readQueue, int bufferSize) {
        this.writeQueue = writeQueue;
        this.readQueue = readQueue;
        this.protocol = new ProtocolWriter(bufferSize);
    }

    public void write(SocketChannel channel) throws IOException {
        writeCurrent();
        writeNext();

        protocol.send(channel);
    }

    private void writeCurrent() {
        if (currentWrite != null && protocol.write(currentWrite.command(), currentWrite.args())) {
            readQueue.add(currentWrite);
            currentWrite = null;
        }
    }

    private void writeNext() {
        Operation task;
        while (currentWrite == null && (task = writeQueue.poll()) != null) {
            if (!protocol.write(task.command(), task.args())) {
                currentWrite = task;
            } else {
                readQueue.add(task);
            }
        }
    }
}
