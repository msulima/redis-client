package pl.msulima.redis.benchmark.log.protocol;

import redis.clients.jedis.Protocol;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class DynamicEncoder {

    public static final Charset CHARSET = StandardCharsets.UTF_8;

    static final byte ASTERISK = (byte) '*';
    static final byte DOLLAR = (byte) '$';
    private static final int[] SIZE_TABLE = {9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, Integer.MAX_VALUE};
    private static final byte[] DIGIT_TENS = {
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
    private static final byte[] DIGIT_ONES = {
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
    private static final byte[] DIGITS = {
            '0', '1', '2', '3', '4', '5',
            '6', '7', '8', '9', 'a', 'b',
            'c', 'd', 'e', 'f', 'g', 'h',
            'i', 'j', 'k', 'l', 'm', 'n',
            'o', 'p', 'q', 'r', 's', 't',
            'u', 'v', 'w', 'x', 'y', 'z'
    };
    private static final byte[] CRLF = new byte[]{'\r', '\n'};
    private static final int CRLF_SIZE = 2;
    static final int MAX_INTEGER_LENGTH = Integer.toString(Integer.MIN_VALUE).length();

    private final byte[] lengthBuf = new byte[MAX_INTEGER_LENGTH];

    private int elementIdx = 0;
    private int offset = 0;

    private byte[][] args;
    private Protocol.Command command;

    public void setRequest(Protocol.Command command, byte[]... args) {
        this.args = args;
        this.command = command;
        this.elementIdx = 0;
    }

    public boolean write(ByteBuffer out) {
        if (command == null) {
            return true;
        }
        if (!writeArraySize(out, args.length + 1))
            return false;

        if (!writeWord(out, command.raw, 0)) {
            return false;
        }

        for (int i = 0; i < args.length; i++) {
            if (!writeWord(out, args[i], i + 1)) {
                return false;
            }
        }

        args = null;
        command = null;
        elementIdx = 0;

        return true;
    }

    private boolean writeArraySize(ByteBuffer out, int value) {
        if (elementIdx > 0) {
            return true;
        }
        if (!atomicIntCrLf(out, ASTERISK, value)) {
            return false;
        }

        elementIdx++;
        return true;
    }

    private boolean writeWord(ByteBuffer out, byte[] word, int i) {
        if (elementIdx > i * 2 + 2) {
            return true;
        }
        if (!writeWordLength(out, word, i)) {
            return false;
        }

        int bytesToWrite = Math.min(word.length - offset, out.remaining());
        out.put(word, offset, bytesToWrite);
        offset += bytesToWrite;

        if (CRLF_SIZE > out.remaining()) {
            return false;
        }

        atomicWriteCrLf(out);

        elementIdx++;
        offset = 0;
        return true;
    }

    private boolean writeWordLength(ByteBuffer out, byte[] word, int i) {
        if (elementIdx > i * 2 + 1) {
            return true;
        }
        if (!atomicIntCrLf(out, DOLLAR, word.length)) {
            return false;
        }
        elementIdx++;
        return true;
    }

    private boolean atomicIntCrLf(ByteBuffer out, byte prefix, int i) {
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
            buf[--charPos] = DIGIT_ONES[r];
            buf[--charPos] = DIGIT_TENS[r];
        }

        // Fall through to fast mode for smaller numbers
        // assert(i <= 65536, i);
        do {
            q = (i * 52429) >>> (16 + 3);
            r = i - ((q << 3) + (q << 1));  // r = i-(q*10) ...
            buf[--charPos] = DIGITS[r];
            i = q;
        } while (i != 0);
        if (sign != 0) {
            buf[--charPos] = sign;
        }
        out.put(buf, 0, size);

        atomicWriteCrLf(out);
        return true;
    }

    private void atomicWriteCrLf(ByteBuffer out) {
        out.put(CRLF);
    }
}
