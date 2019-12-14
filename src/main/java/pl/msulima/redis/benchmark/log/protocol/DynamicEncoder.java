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
    static final int MAX_INTEGER_LENGTH = Integer.toString(Integer.MAX_VALUE).length();

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
        if (elementIdx == 0) {
            if (writeArraySize(out, args.length + 1)) {
                elementIdx++;
            } else {
                return false;
            }
        }
        if (elementIdx == 1) {
            if (writeWordLength(out, command.raw)) {
                elementIdx++;
            } else {
                return false;
            }
        }
        if (elementIdx == 2) {
            if (writeWord(out, command.raw)) {
                elementIdx++;
            } else {
                return false;
            }
        }

        for (int i = 0; i < args.length; i++) {
            if (elementIdx == 3 + i * 2) {
                if (writeWordLength(out, args[i])) {
                    elementIdx++;
                } else {
                    return false;
                }
            }
            if (elementIdx == 4 + i * 2) {
                if (writeWord(out, args[i])) {
                    elementIdx++;
                } else {
                    return false;
                }
            }
        }

        args = null;
        command = null;
        elementIdx = 0;

        return true;
    }

    private boolean writeArraySize(ByteBuffer out, int value) {
        return writeSize(out, ASTERISK, value);
    }

    private boolean writeWordLength(ByteBuffer out, byte[] word) {
        return writeSize(out, DOLLAR, word.length);
    }

    private boolean writeSize(ByteBuffer out, byte prefix, int value) {
        if (1 + MAX_INTEGER_LENGTH + CRLF_SIZE > out.remaining()) {
            return false;
        }
        out.put(prefix);

        final int size = sizeInteger(value);
        int q, r;
        int charPos = size;
        byte[] buf = this.lengthBuf;

        // Generate two digits per iteration
        while (value >= 65536) {
            q = value / 100;
            // really: r = i - (q * 100);
            r = value - ((q << 6) + (q << 5) + (q << 2));
            value = q;
            buf[--charPos] = DIGIT_ONES[r];
            buf[--charPos] = DIGIT_TENS[r];
        }

        // Fall through to fast mode for smaller numbers
        // assert(i <= 65536, i);
        do {
            q = (value * 52429) >>> (16 + 3);
            r = value - ((q << 3) + (q << 1));  // r = i-(q*10) ...
            buf[--charPos] = DIGITS[r];
            value = q;
        } while (value != 0);
        out.put(buf, 0, size);

        writeCrLf(out);
        return true;
    }

    private boolean writeWord(ByteBuffer out, byte[] word) {
        int bytesToWrite = Math.min(word.length - offset, out.remaining());
        out.put(word, offset, bytesToWrite);
        offset += bytesToWrite;

        if (CRLF_SIZE > out.remaining()) {
            return false;
        }

        writeCrLf(out);

        offset = 0;
        return true;
    }

    private void writeCrLf(ByteBuffer out) {
        out.put(CRLF);
    }

    private int sizeInteger(int value) {
        // value can be only non-negative
        int size = 0;
        while (value > SIZE_TABLE[size]) {
            size++;
        }
        size++;
        return size;
    }
}
