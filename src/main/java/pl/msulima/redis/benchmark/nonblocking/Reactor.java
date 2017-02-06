package pl.msulima.redis.benchmark.nonblocking;

import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;


public class Reactor implements Runnable {

    public static final int OPERATIONS = SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE;
    public static final int MAX_REQUESTS = 10 * 1024 * 1024;
    public static final int BUFFER_SIZE = 4 * 1024;

    private final ManyToOneConcurrentArrayQueue<Operation> writeQueue = new ManyToOneConcurrentArrayQueue<>(MAX_REQUESTS);
    private final OneToOneConcurrentArrayQueue<Operation> readQueue = new OneToOneConcurrentArrayQueue<>(MAX_REQUESTS);

    private final int connectionsCount;
    private final int port;

    private final Writer writer = new Writer(writeQueue, readQueue, BUFFER_SIZE);
    private final Reader reader = new Reader(readQueue, BUFFER_SIZE);

    public Reactor(int port) {
        this.port = port;
        this.connectionsCount = 1;
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

    private void submit(Operation task) {
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
        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        channel.setOption(StandardSocketOptions.SO_RCVBUF, BUFFER_SIZE);
        channel.setOption(StandardSocketOptions.SO_SNDBUF, BUFFER_SIZE);
        channel.connect(serverAddress);

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
                reader.read(channel);
            }
            if (key.isWritable()) {
                writer.write(channel);
            }
        }
    }

    private void connect(SelectionKey key, SocketChannel channel) throws IOException {
        while (channel.isConnectionPending()) {
            channel.finishConnect();
            key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
            System.out.printf("C Connected to %d%n", port);
        }
    }

    private void closeConnections(Selector selector) throws IOException {
        for (SelectionKey key : selector.keys()) {
            SocketChannel channel = (SocketChannel) key.channel();
            channel.close();
        }
    }
}
