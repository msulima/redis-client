package pl.msulima.redis.benchmark.nonblocking;

import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;


public class Reactor implements Runnable {

    public static final int MAX_REQUESTS = 10 * 1024 * 1024;
    public static final int BUFFER_SIZE = 128 * 1024;

    private final ManyToOneConcurrentArrayQueue[] writeQueues;
    private final String host;
    private final int port;
    private final int concurrency;
    private int idx;

    public Reactor(String host, int port, int concurrency) {
        this.host = host;
        this.port = port;
        this.concurrency = concurrency;
        this.writeQueues = new ManyToOneConcurrentArrayQueue[concurrency];

        for (int i = 0; i < concurrency; i++) {
            this.writeQueues[i] = new ManyToOneConcurrentArrayQueue<Operation>(MAX_REQUESTS);
        }
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

    @SuppressWarnings("unchecked")
    private void submit(Operation task) {
        writeQueues[idx++ % concurrency].add(task);
    }

    private void runInternal() throws Exception {
        Selector selector = Selector.open();

        for (int i = 0; i < concurrency; i++) {
            createConnection(selector, i);
        }

        while (!Thread.interrupted()) {
            if (selector.select() > 0) {
                processReadySet(selector.selectedKeys());
            }
        }

        closeConnections(selector);
        selector.close();
    }

    private void createConnection(Selector selector, int i) throws IOException {
        Queue<Operation> readQueue = new ArrayDeque<>(MAX_REQUESTS);

        Writer writer = new Writer(writeQueues[i], readQueue, BUFFER_SIZE);
        Reader reader = new Reader(readQueue, BUFFER_SIZE);

        Connection connection = new Connection(writer, reader);

        InetSocketAddress serverAddress = new InetSocketAddress(InetAddress.getByName(host), this.port + i);
        SocketChannel channel = createChannel(serverAddress);
        SelectionKey selectionKey = channel.register(selector, SelectionKey.OP_CONNECT);
        selectionKey.attach(connection);
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

    private void processReadySet(Set<SelectionKey> readySet) throws IOException {
        Iterator<SelectionKey> iterator = readySet.iterator();

        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove();
            SocketChannel channel = (SocketChannel) key.channel();

            processChannel(key, channel);
        }
    }

    private void processChannel(SelectionKey key, SocketChannel channel) throws IOException {
        if (key.isConnectable()) {
            connect(key, channel);
        }
        if (key.isReadable()) {
            ((Connection) key.attachment()).read(channel);
        }
        if (key.isWritable()) {
            ((Connection) key.attachment()).write(channel);
        }
    }

    private void connect(SelectionKey key, SocketChannel channel) {
        while (channel.isConnectionPending()) {
            try {
                channel.finishConnect();
            } catch (IOException e) {
                throw new RuntimeException("Could not connect to " + host + " " + port, e);
            }
            key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
            System.out.printf("C Connected to %s:%d%n", host, port);
        }
    }

    private void closeConnections(Selector selector) throws IOException {
        for (SelectionKey key : selector.keys()) {
            SocketChannel channel = (SocketChannel) key.channel();
            channel.close();
        }
    }
}
