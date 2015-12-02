package pl.msulima.redis.benchmark.nio;

import pl.msulima.redis.benchmark.jedis.Operation;
import uk.co.real_logic.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.WritableByteChannel;

public class Writer {

    private final ManyToOneConcurrentArrayQueue<Operation> commands;
    private final OneToOneConcurrentArrayQueue<Operation> pending;

    public Writer(ManyToOneConcurrentArrayQueue<Operation> commands, OneToOneConcurrentArrayQueue<Operation> pending) {
        this.commands = commands;
        this.pending = pending;
    }

    public void write(SelectionKey key) throws IOException {
        WritableByteChannel channel = (WritableByteChannel) key.channel();

        Operation command;
        while ((command = commands.poll()) != null) {
            writeOne(command, channel);
            pending.offer(command);
        }

        if (!pending.isEmpty()) {
            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        }
    }

    private void writeOne(Operation command, WritableByteChannel channel) throws IOException {
        byte[] bytes = command.getBytes();
        channel.write(ByteBuffer.wrap(bytes));
        System.out.print("-> " + new String(bytes));
    }
}