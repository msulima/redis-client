package pl.msulima.redis.benchmark.nonblocking;

import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public class Writer {

    private final ManyToOneConcurrentArrayQueue<Operation> writeQueue;
    private final OneToOneConcurrentArrayQueue<Operation> readQueue;
    private final ProtocolWriter protocol;

    private Operation currentWrite;

    public Writer(ManyToOneConcurrentArrayQueue<Operation> writeQueue, OneToOneConcurrentArrayQueue<Operation> readQueue, int bufferSize) {
        this.writeQueue = writeQueue;
        this.readQueue = readQueue;
        this.protocol = new ProtocolWriter(bufferSize);
    }

    public void write(SocketChannel channel) throws IOException {
        boolean anyWritten = writeCurrent();
        anyWritten = anyWritten | writeNext();

        if (anyWritten) {
            protocol.send(channel);
        }
    }

    private boolean writeCurrent() {
        boolean anyWritten = currentWrite != null;
        if (anyWritten && protocol.write(currentWrite.command(), currentWrite.args())) {
            readQueue.add(currentWrite);
            currentWrite = null;
            return true;
        }
        return anyWritten;
    }

    private boolean writeNext() {
        Operation task;
        boolean anyWritten = false;
        while (currentWrite == null && (task = writeQueue.poll()) != null) {
            if (!protocol.write(task.command(), task.args())) {
                currentWrite = task;
            } else {
                readQueue.add(task);
            }
            anyWritten = true;
        }
        return anyWritten;
    }
}
