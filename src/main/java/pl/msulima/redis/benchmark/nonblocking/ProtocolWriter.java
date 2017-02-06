package pl.msulima.redis.benchmark.nonblocking;

import redis.clients.jedis.Protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

public class ProtocolWriter {

    public static final int MAX_INTEGER_LENGTH = Integer.toString(Integer.MIN_VALUE).length();

    private static final byte ASTERISK = (byte) '*';
    private static final byte[] CRLF = new byte[]{'\r', '\n'};

    private static final int[] SIZE_TABLE = {9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, Integer.MAX_VALUE};
    private static final byte[] DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
    private static final byte DOLLAR = (byte) '$';

    private final ByteBuffer out;
    private final byte[] lengthBuf = new byte[MAX_INTEGER_LENGTH];
    private static final int CRLF_SIZE = 2;
    private static final int MAX_HEADER_SIZE = 1 + MAX_INTEGER_LENGTH + CRLF_SIZE;

    private int elementIdx = 0;
    private int offset = 0;

    public ProtocolWriter(ByteBuffer out) {
        this.out = out;
    }

    public ProtocolWriter(int size) {
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

    final static byte[] DigitTens = {
            '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
            '1', '1', '1', '1', '1', '1', '1', '1', '1', '1',
            '2', '2', '2', '2', '2', '2', '2', '2', '2', '2',
            '3', '3', '3', '3', '3', '3', '3', '3', '3', '3',
            '4', '4', '4', '4', '4', '4', '4', '4', '4', '4',
            '5', '5', '5', '5', '5', '5', '5', '5', '5', '5',
            '6', '6', '6', '6', '6', '6', '6', '6', '6', '6',
            '7', '7', '7', '7', '7', '7', '7', '7', '7', '7',
            '8', '8', '8', '8', '8', '8', '8', '8', '8', '8',
            '9', '9', '9', '9', '9', '9', '9', '9', '9', '9',
    };

    final static byte[] DigitOnes = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    };
    final static byte[] digits = {
            '0', '1', '2', '3', '4', '5',
            '6', '7', '8', '9', 'a', 'b',
            'c', 'd', 'e', 'f', 'g', 'h',
            'i', 'j', 'k', 'l', 'm', 'n',
            'o', 'p', 'q', 'r', 's', 't',
            'u', 'v', 'w', 'x', 'y', 'z'
    };

    private boolean atomicIntCrLf(byte prefix, int i) {
        // value can be only non-negative
        int size = 0;
        while (i > SIZE_TABLE[size]) {
            size++;
        }
        size++;

        if (size + 3 > out.remaining()) {
            return false;
        }

        out.put(prefix);

        int q, r;
        int charPos = size;
        byte sign = 0;
        byte[] buf = this.lengthBuf;

        if (i < 0) {
            sign = '-';
            i = -i;
        }

        // Generate two digits per iteration
        while (i >= 65536) {
            q = i / 100;
            // really: r = i - (q * 100);
            r = i - ((q << 6) + (q << 5) + (q << 2));
            i = q;
            buf[--charPos] = DigitOnes[r];
            buf[--charPos] = DigitTens[r];
        }

        // Fall thru to fast mode for smaller numbers
        // assert(i <= 65536, i);
        for (; ; ) {
            q = (i * 52429) >>> (16 + 3);
            r = i - ((q << 3) + (q << 1));  // r = i-(q*10) ...
            buf[--charPos] = digits[r];
            i = q;
            if (i == 0) break;
        }
        if (sign != 0) {
            buf[--charPos] = sign;
        }
        out.put(buf, 0, size);

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
