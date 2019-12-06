package pl.msulima.redis.benchmark.log.protocol;

import redis.clients.jedis.Protocol;

import java.nio.ByteBuffer;

public class Encoder {

    private static final byte ASTERISK = (byte) '*';
    private static final byte DOLLAR = (byte) '$';
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

    public static byte[] write(Protocol.Command command, byte[]... args) {
        byte[] array = new byte[calculateSize(command, args)];

        ByteBuffer out = ByteBuffer.wrap(array);
        writeArraySize(out, args);
        writeWord(out, command.raw);
        for (byte[] arg : args) {
            writeWord(out, arg);
        }

        return out.array();
    }

    private static int calculateSize(Protocol.Command command, byte[][] args) {
        int argumentsSize = 0;
        for (byte[] arg : args) {
            argumentsSize += sizeWord(arg);
        }
        return sizeArraySize(args) + sizeWord(command.raw) + argumentsSize;
    }

    private static int sizeArraySize(byte[][] args) {
        return 1 + calculateSize(args.length + 1) + CRLF.length;
    }

    private static void writeArraySize(ByteBuffer out, byte[][] args) {
        out.put(ASTERISK);
        writeInt(out, args.length + 1);
        writeCrLf(out);
    }

    private static int calculateSize(int i) {
        // value can be only non-negative
        int size = 0;
        while (i > SIZE_TABLE[size]) {
            size++;
        }
        size++;
        return size;
    }

    private static int sizeWord(byte[] word) {
        return sizeWordLength(word) + word.length + CRLF.length;
    }

    private static int sizeWordLength(byte[] word) {
        return 1 + calculateSize(word.length + 1) + CRLF.length;
    }

    private static void writeWord(ByteBuffer out, byte[] word) {
        writeWordLength(out, word);
        out.put(word);
        writeCrLf(out);
    }

    private static void writeWordLength(ByteBuffer out, byte[] word) {
        out.put(DOLLAR);
        writeInt(out, word.length);
        writeCrLf(out);
    }

    private static void writeInt(ByteBuffer out, int i) {
        int size = calculateSize(i);

        int q, r;
        int charPos = out.position() + size;
        byte[] buf = out.array();

        // Generate two digits per iteration
        while (i >= 65536) {
            q = i / 100;
            // really: r = i - (q * 100);
            r = i - ((q << 6) + (q << 5) + (q << 2));
            i = q;
            buf[--charPos] = DIGIT_ONES[r];
            buf[--charPos] = DIGIT_TENS[r];
        }

        // Fall thru to fast mode for smaller numbers
        // assert(i <= 65536, i);
        do {
            q = (i * 52429) >>> (16 + 3);
            r = i - ((q << 3) + (q << 1));  // r = i-(q*10) ...
            buf[--charPos] = DIGITS[r];
            i = q;
        } while (i != 0);
        out.position(out.position() + size);
    }

    private static void writeCrLf(ByteBuffer out) {
        out.put(CRLF);
    }
}
