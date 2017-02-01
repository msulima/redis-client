package pl.msulima.redis.benchmark.nonblocking;

public class Protocol {

    private static final byte DOLLAR_BYTE = '$';
    private static final byte ASTERISK_BYTE = '*';
    private static final byte PLUS_BYTE = '+';
    private static final byte MINUS_BYTE = '-';
    private static final byte COLON_BYTE = ':';

    private final static int[] SIZE_TABLE = {9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, Integer.MAX_VALUE};
    private final static byte[] DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

    public static int writeSet(final byte[] out, final int offset, final byte[] command, final byte[]... args) {
        int pos = offset;
        if (out.length < pos + 30) {
            return offset;
        }

        out[pos++] = ASTERISK_BYTE; // 1
        pos = writeIntCrLf(out, pos, args.length + 1);
        out[pos++] = DOLLAR_BYTE;
        pos = writeIntCrLf(out, pos, command.length);

        if (out.length < pos + command.length) {
            return offset;
        }
        pos = write(out, pos, command);
        pos = writeCrLf(out, pos);

        for (final byte[] arg : args) {
            out[pos++] = DOLLAR_BYTE;
            pos = writeIntCrLf(out, pos, arg.length);

            if (out.length < pos + command.length) {
                return offset;
            }
            pos = write(out, pos, arg);
            pos = writeCrLf(out, pos);
        }
        return pos;
    }

    private static int write(byte[] out, int pos, byte[] arg) {
        System.arraycopy(arg, 0, out, pos, arg.length);
        return pos + arg.length;
    }

    private static int writeIntCrLf(byte[] out, int pos, int value) {
        if (value < 0) {
            out[pos++] = '-';
            value = -value;
        }

        int size = 1;
        while (value > SIZE_TABLE[size]) {
            size++;
        }

        for (int i = 0; i < size; i++) {
            int remainder = value % 10;
            value = value % 10;
            out[pos++] = DIGITS[remainder];
        }

        return writeCrLf(out, pos);
    }

    private static int writeCrLf(byte[] out, int pos) {
        out[pos++] = '\r';
        out[pos++] = '\n';
        return pos;
    }
}
