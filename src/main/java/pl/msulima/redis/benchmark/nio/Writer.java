package pl.msulima.redis.benchmark.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.WritableByteChannel;
import java.util.Queue;

public class Writer {

    public static final byte DOLLAR_BYTE = '$';
    public static final byte ASTERISK_BYTE = '*';
    public static final byte[] CR_LF = new byte[]{'\r', '\n'};

    private final Queue<Operation> commands;
    private final Queue<Operation> pending;

    private final ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
    private final int maxBytesWritten = Integer.parseInt(System.getProperty("maxBytesWritten", "0"));

    public Writer(Queue<Operation> commands, Queue<Operation> pending) {
        this.commands = commands;
        this.pending = pending;
        buffer.flip();
    }

    public void write(SelectionKey key) throws IOException {
        WritableByteChannel channel = (WritableByteChannel) key.channel();

        if (buffer.hasRemaining()) {
            writeTo(channel);
        }

        Operation command;
        while (!buffer.hasRemaining() && (command = commands.poll()) != null) {
            writeOne(command, channel);
            pending.add(command);
        }

        if (!pending.isEmpty()) {
            key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        }
    }

    private void writeOne(Operation command, WritableByteChannel channel) throws IOException {
        buffer.clear();
        command.writeTo(buffer);

        buffer.flip();
        int write = writeTo(channel);

        if (write < 0) {
            throw new IllegalStateException(write + " bytes written.");
        }
    }

    private int writeTo(WritableByteChannel channel) throws IOException {
        int bytesWritten;

        if (maxBytesWritten > 0) {
            int limit = buffer.limit();
            int position = buffer.position();
            buffer.limit(Math.min(position + maxBytesWritten, buffer.limit()));

            bytesWritten = channel.write(buffer);

            buffer.limit(limit);
        } else {
            bytesWritten = channel.write(buffer);
        }

        return bytesWritten;
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