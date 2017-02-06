package pl.msulima.redis.benchmark.nonblocking;

import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;


public class Reactor implements Runnable {

    public static final int OPERATIONS = SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE;
    public static final int MAX_REQUESTS = 10 * 1024 * 1024;

    private final Queue<Operation> writeQueue = new ManyToOneConcurrentArrayQueue<>(MAX_REQUESTS);
    private final Queue<Operation> readQueue = new ManyToOneConcurrentArrayQueue<>(MAX_REQUESTS);

    private final int connectionsCount = 1;
    private final int port;

    private final ProtocolWriter writer = new ProtocolWriter(128 * 1024);
    private final ProtocolReader reader = new ProtocolReader(128 * 1024);

    private Operation currentWrite;
    private Operation currentRead;

    public Reactor(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        try {
            runInternal();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void set(byte[] key, byte[] value, Runnable onComplete) {
        submit(Operation.set(key, value, onComplete));
    }

    public void get(byte[] key, Consumer<byte[]> callback) {
        submit(Operation.get(key, callback));
    }

    public void submit(Operation task) {
        writeQueue.add(task);
    }

    private void runInternal() throws Exception {
        InetSocketAddress serverAddress = new InetSocketAddress(port);
        Selector selector = Selector.open();

        for (int i = 0; i < connectionsCount; i++) {
            SocketChannel channel = createChannel(serverAddress);
            channel.register(selector, OPERATIONS);
        }

        while (!Thread.interrupted()) {
            if (selector.select() > 0) {
                processReadySet(selector.selectedKeys());
            }
        }

        closeConnections(selector);
        selector.close();
    }

    private SocketChannel createChannel(InetSocketAddress serverAddress) throws IOException {
        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.connect(serverAddress);
        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);

        return channel;
    }

    private void processReadySet(Set readySet) throws IOException {
        Iterator iterator = readySet.iterator();

        while (iterator.hasNext()) {
            SelectionKey key = (SelectionKey) iterator.next();
            iterator.remove();

            SocketChannel channel = (SocketChannel) key.channel();

            if (key.isConnectable()) {
                connect(key, channel);
            }
            if (key.isReadable()) {
                read(channel);
            }
            if (key.isWritable()) {
                maybeWrite(channel);
            }
        }
    }

    private void connect(SelectionKey key, SocketChannel channel) throws IOException {
        while (channel.isConnectionPending()) {
            channel.finishConnect();
            key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
            System.out.printf("C Connected to %d%n", 6379);
        }
    }

    private void read(SocketChannel channel) throws IOException {
        if (reader.receive(channel) == 0) {
            return;
        }

        Response response = new Response();

        if (currentRead != null && reader.read(response)) {
            currentRead.finish(response);
            currentRead = null;
        }

        Operation task;
        while (currentRead == null && (task = readQueue.poll()) != null) {
            if (!reader.read(response)) {
                currentRead = task;
            } else {
                task.finish(response);
            }
        }
    }

    private void maybeWrite(SocketChannel channel) throws IOException {
        writeCurrent();
        writeNext();

        writer.send(channel);
    }

    private void writeCurrent() {
        if (currentWrite != null && writer.write(currentWrite.command(), currentWrite.args())) {
            readQueue.add(currentWrite);
            currentWrite = null;
        }
    }

    private void writeNext() {
        Operation task;
        while (currentWrite == null && (task = writeQueue.poll()) != null) {
            if (!writer.write(task.command(), task.args())) {
                currentWrite = task;
            } else {
                readQueue.add(task);
            }
        }
    }

    private void closeConnections(Selector selector) throws IOException {
        for (SelectionKey key : selector.keys()) {
            SocketChannel channel = (SocketChannel) key.channel();
            channel.close();
        }
    }
}
