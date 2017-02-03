package pl.msulima.redis.benchmark.nonblocking;

import redis.clients.jedis.Protocol;

import java.nio.ByteBuffer;

public class ProtocolByteBufferWriter {

    private static final byte ASTERISK = (byte) '*';
    private static final byte[] CRLF = new byte[]{'\r', '\n'};

    private static final int[] SIZE_TABLE = {9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, Integer.MAX_VALUE};
    private static final byte[] DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
    private static final byte DOLLAR = (byte) '$';
    private static final int MAX_INTEGER_LENGTH = Integer.toString(Integer.MAX_VALUE).length();

    private final ByteBuffer out;
    public static final int MAX_ARRAY_HEADER_SIZE = 1 + MAX_INTEGER_LENGTH + 2;

    private int word = 0;
    private int offset = 0;

    public ProtocolByteBufferWriter(ByteBuffer out) {
        this.out = out;
    }

    public boolean write(final Protocol.Command command, final byte[]... args) {
        if (MAX_ARRAY_HEADER_SIZE > out.remaining()) {
            return false;
        }
        writeArraySize(args.length + 1);
        word++;

        if (wordSize(command.raw) > out.remaining()) {
            return false;
        }
        writeWord(command.raw);
        word++;

        for (byte[] arg : args) {
            if (wordSize(arg) > out.remaining()) {
                return false;
            }
            writeWord(arg);
            word++;
        }

        return true;
    }

    private int wordSize(byte[] word) {
        return 1 + MAX_INTEGER_LENGTH + 2 + word.length + 2;
    }

    private void writeArraySize(int value) {
        out.put(ASTERISK);
        writeIntCrLf(value);
    }

    private void writeWord(byte[] word) {
        out.put(DOLLAR);
        writeIntCrLf(word.length);
        out.put(word);
        writeCrLf();
    }

    private void writeIntCrLf(int value) {
        // value can be only non-negative
        int size = 1;
        while (value > SIZE_TABLE[size]) {
            size++;
        }

        for (int i = 0; i < size; i++) {
            int remainder = value % 10;
            value = value % 10;
            out.put(DIGITS[remainder]);
        }

        writeCrLf();
    }

    private void writeCrLf() {
        out.put(CRLF);
    }
}
