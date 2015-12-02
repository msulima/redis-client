package pl.msulima.redis.benchmark.nio;

import pl.msulima.redis.benchmark.jedis.Operation;
import uk.co.real_logic.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;


class Connection implements Runnable {

    public static final int TIMEOUT = 1000;
    private Selector selector;
    private final Writer writer;
    private final Reader reader;
    private final ManyToOneConcurrentArrayQueue<Operation> commands;

    public Connection() {
        commands = new ManyToOneConcurrentArrayQueue<>(10_000);
        OneToOneConcurrentArrayQueue<Operation> pending = new OneToOneConcurrentArrayQueue<>(10_000);

        this.writer = new Writer(this.commands, pending);
        this.reader = new Reader(pending);

        Thread thread = new Thread(this);
        thread.start();
    }

    public void submit(Operation operation) {
        commands.offer(operation);
    }

    @Override
    public void run() {
        SocketChannel channel;
        try {
            selector = Selector.open();
            channel = SocketChannel.open();
            channel.configureBlocking(false);

            channel.register(selector, SelectionKey.OP_CONNECT);
            channel.connect(new InetSocketAddress("127.0.0.1", 6379));

            while (!Thread.interrupted()) {
                selector.select(TIMEOUT);

                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();

                    if (!key.isValid()) continue;

                    if (key.isConnectable()) {
                        System.out.println("I am connected to the server");
                        connect(key);
                    }
                    if (key.isReadable()) {
                        reader.read(key);
                    }
                    if (key.isWritable()) {
                        writer.write(key);
                    }
                }
            }
        } catch (IOException e1) {
            e1.printStackTrace();
        } finally {
            close();
        }
    }

    private void connect(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        if (channel.isConnectionPending()) {
            channel.finishConnect();
        }
        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_WRITE);
    }

    private void close() {
        try {
            selector.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
