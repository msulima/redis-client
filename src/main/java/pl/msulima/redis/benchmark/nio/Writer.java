package pl.msulima.redis.benchmark.nio;

import pl.msulima.redis.benchmark.jedis.Operation;
import uk.co.real_logic.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.WritableByteChannel;

public class Writer {

    public static final byte DOLLAR_BYTE = '$';
    public static final byte ASTERISK_BYTE = '*';
    public static final byte[] CR_LF = new byte[]{'\r', '\n'};

    private final ManyToOneConcurrentArrayQueue<Operation> commands;
    private final OneToOneConcurrentArrayQueue<Operation> pending;

    private final ByteBuffer buffer = ByteBuffer.allocate(1024);

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
        buffer.clear();
        command.writeTo(buffer);

        buffer.flip();
        System.out.print("-> " + buffer.limit() + " " + new String(buffer.array(), 0, buffer.limit()));
        channel.write(buffer);
    }

    public static void sendCommand(ByteBuffer buffer, String command, byte[]... args) {
        byte[] encodedCommand = Encoder.encode(command);
        buffer.put(ASTERISK_BYTE);
        writeIntCrLf(buffer, args.length + 1);
        buffer.put(DOLLAR_BYTE);
        writeIntCrLf(buffer, encodedCommand.length);
        buffer.put(encodedCommand);
        writeCrLf(buffer);

        for (final byte[] arg : args) {
            buffer.put(DOLLAR_BYTE);
            writeIntCrLf(buffer, arg.length);
            buffer.put(arg);
            writeCrLf(buffer);
        }
    }

    private static void writeIntCrLf(ByteBuffer buffer, int length) {
        buffer.put(Encoder.encode(Integer.toString(length)));
        writeCrLf(buffer);
    }

    private static void writeCrLf(ByteBuffer buffer) {
        buffer.put(CR_LF);
    }

}