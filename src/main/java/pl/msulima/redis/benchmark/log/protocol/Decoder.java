package pl.msulima.redis.benchmark.log.protocol;

import com.google.common.base.Charsets;

import java.nio.ByteBuffer;

public class Decoder {

    public static Response read(byte[] array) {
        ByteBuffer in = ByteBuffer.wrap(array);

        byte read = in.get();

        switch (read) {
            case '+':
                return simpleString(in);
            case '$':
                return bulkStringStart(in);
            default:
                throw new RuntimeException("Could not read response " + read);
        }
    }

    private static Response bulkStringStart(ByteBuffer in) {
        int length = readLength(in);
        if (length == -1) {
            return Response.nullResponse();
        }

        byte[] result = new byte[length];
        System.arraycopy(in.array(), in.position() + 1, result, 0, length);
        return Response.bulkString(result);
    }

    private static Response simpleString(ByteBuffer in) {
        return Response.simpleString(new String(in.array(), in.position(), in.capacity() - in.position() - 2, Charsets.US_ASCII));
    }

    private static int readLength(ByteBuffer in) {
        int result = 0;
        byte sign = 1;

        byte b = in.get();
        if (b == '-') {
            sign = -1;
        } else {
            result = b - '0';
        }

        while ((b = in.get()) != '\r') {
            result = result * 10 + (b - '0');
        }

        if (sign == -1) {
            return -result;
        } else {
            return result;
        }
    }
}
