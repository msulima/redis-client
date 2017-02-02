package pl.msulima.redis.benchmark.nonblocking;

import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;


public class Reactor implements Runnable {

    public static final int TIMEOUT = 100;
    public static final int OPERATIONS = SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE;
    public static final int MAX_REQUESTS = 8 * 1024;

    private final Queue<Operation> tasks = new ManyToOneConcurrentArrayQueue<>(MAX_REQUESTS);
    private final Queue<Operation> pendingOperations = new ManyToOneConcurrentArrayQueue<>(MAX_REQUESTS);

    private final int connectionsCount = 1;
    private final int port;
    private final byte[] writeBuffer = new byte[2 * 1024];
    private final byte[] readBuffer = new byte[2 * 1024];
    private final ByteBuffer writeByteBuffer = ByteBuffer.wrap(writeBuffer);
    private final ByteBuffer readByteBuffer = ByteBuffer.wrap(readBuffer);

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

    public void submit(Operation task) {
        tasks.add(task);
    }

    private void runInternal() throws Exception {
        InetSocketAddress serverAddress = new InetSocketAddress(port);
        Selector selector = Selector.open();

        for (int i = 0; i < connectionsCount; i++) {
            SocketChannel channel = createChannel(serverAddress);
            channel.register(selector, OPERATIONS);
        }

        while (!Thread.interrupted()) {
            if (selector.select(TIMEOUT) > 0) {
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
                read(key, channel);
            }
            if (key.isWritable()) {
                maybeWrite(key, channel);
            }
        }
    }

    private void connect(SelectionKey key, SocketChannel channel) throws IOException {
        while (channel.isConnectionPending()) {
            channel.finishConnect();
            key.interestOps(SelectionKey.OP_WRITE);
            System.out.printf("C Connected to %d%n", 6379);
        }
    }

    private void read(SelectionKey key, SocketChannel channel) throws IOException {
        readByteBuffer.clear();
        channel.read(readByteBuffer);
        Response response = new Response();
        ProtocolReader.read(readBuffer, 0, response);

        pendingOperations.poll().finish(response);

        key.interestOps(SelectionKey.OP_WRITE);
    }

    private void maybeWrite(SelectionKey key, SocketChannel channel) throws IOException {
        Operation task = tasks.poll();

        if (task != null) {
            pendingOperations.add(task);

            writeByteBuffer.clear();
            int newPosition = ProtocolWriter.write(writeBuffer, 0, task.command(), task.args());
            writeByteBuffer.position(newPosition);
            writeByteBuffer.flip();

            channel.write(writeByteBuffer);

            key.interestOps(SelectionKey.OP_READ);
        }
    }

    private void closeConnections(Selector selector) throws IOException {
        for (SelectionKey key : selector.keys()) {
            SocketChannel channel = (SocketChannel) key.channel();
            channel.close();
        }
    }
}
