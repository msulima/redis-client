package pl.msulima.redis.benchmark.nonblocking;

import redis.clients.jedis.Protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

public class ProtocolByteBufferWriter {

    public static final int MAX_INTEGER_LENGTH = Integer.toString(Integer.MIN_VALUE).length();

    private static final byte ASTERISK = (byte) '*';
    private static final byte[] CRLF = new byte[]{'\r', '\n'};

    private static final int[] SIZE_TABLE = {9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, Integer.MAX_VALUE};
    private static final byte[] DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
    private static final byte DOLLAR = (byte) '$';

    private final ByteBuffer out;
    private static final int CRLF_SIZE = 2;
    private static final int MAX_HEADER_SIZE = 1 + MAX_INTEGER_LENGTH + CRLF_SIZE;

    private int elementIdx = 0;
    private int offset = 0;

    public ProtocolByteBufferWriter(ByteBuffer out) {
        this.out = out;
    }

    public ProtocolByteBufferWriter(int size) {
        this.out = ByteBuffer.allocate(size);
    }

    public void read() {
    }

    public boolean write(final Protocol.Command command, final byte[]... args) {
        if (!writeArraySize(args.length + 1))
            return false;

        if (!writeWord(command.raw, 0)) {
            return false;
        }

        for (int i = 0; i < args.length; i++) {
            if (!writeWord(args[i], i + 1)) {
                return false;
            }
        }

        elementIdx = 0;
        return true;
    }

    private boolean writeArraySize(int value) {
        if (elementIdx > 0) {
            return true;
        }
        if (!atomicIntCrLf(ASTERISK, value)) {
            return false;
        }

        elementIdx++;
        return true;
    }

    private boolean writeWord(byte[] word, int i) {
        if (elementIdx > i * 2 + 2) {
            return true;
        }
        if (!writeWordLength(word, i)) {
            return false;
        }

        int bytesToWrite = Math.min(word.length - offset, out.remaining());
        out.put(word, offset, bytesToWrite);
        offset += bytesToWrite;

        if (CRLF_SIZE > out.remaining()) {
            return false;
        }

        atomicWriteCrLf();

        elementIdx++;
        offset = 0;
        return true;
    }

    private boolean writeWordLength(byte[] word, int i) {
        if (elementIdx > i * 2 + 1) {
            return true;
        }
        if (!atomicIntCrLf(DOLLAR, word.length)) {
            return false;
        }
        elementIdx++;
        return true;
    }

    private boolean atomicIntCrLf(byte prefix, int value) {
        if (MAX_HEADER_SIZE > out.remaining()) {
            return false;
        }

        out.put(prefix);

        // value can be only non-negative
        int size = 0;
        while (value > SIZE_TABLE[size]) {
            size++;
        }
        size++;

        byte[] buf = new byte[size];
        while (size > 0) {
            int remainder = value % 10;
            value = value / 10;
            buf[--size] = DIGITS[remainder];
        }
        out.put(buf);

        atomicWriteCrLf();
        return true;
    }

    private void atomicWriteCrLf() {
        out.put(CRLF);
    }

    public void send(ByteChannel channel) throws IOException {
        out.flip();
        channel.write(out);
        out.compact();
    }
}
