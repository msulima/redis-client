package pl.msulima.redis.benchmark.nio;


import java.nio.ByteBuffer;
import java.util.Optional;

public class Parser {

    public static final int CR_LF_LENGTH = 2;
    public static final int NULL_BULK_STRING_LENGTH = -1;

    public Optional<?> parse(ByteBuffer buffer) {
        int position = buffer.position();
        int lineEnd = findLineEnd(buffer);
        if (lineEnd > position) {
            char b = (char) buffer.get();
            switch (b) {
                case ':':
                    return Optional.of(readInteger(buffer, lineEnd));
                case '$':
                    return readBulkString(buffer, lineEnd);
                case '+':
                    return Optional.of(readSimpleString(buffer, lineEnd));
                default:
                    throw new RuntimeException("wtf" + b);
            }
        }
        return Optional.empty();
    }

    private Integer readInteger(ByteBuffer buffer, int lineEnd) {
        return Integer.parseInt(readSimpleString(buffer, lineEnd));
    }

    private Optional<Optional<byte[]>> readBulkString(ByteBuffer buffer, int lineEnd) {
        int length = Integer.parseInt(readSimpleString(buffer, lineEnd));

        if (length == NULL_BULK_STRING_LENGTH) {
            return Optional.of(Optional.empty());
        } else if (length + CR_LF_LENGTH > buffer.remaining()) {
            return Optional.empty();
        }

        return Optional.of(Optional.of(readBytes(buffer, buffer.position() + length)));
    }

    private String readSimpleString(ByteBuffer buffer, int lineEnd) {
        return new String(readBytes(buffer, lineEnd));
    }

    private byte[] readBytes(ByteBuffer buffer, int lineEnd) {
        int start = buffer.position();
        int length = lineEnd - start;
        byte[] dest = new byte[length];
        System.arraycopy(buffer.array(), start, dest, 0, length);
        buffer.position(Math.min(buffer.limit(), lineEnd + CR_LF_LENGTH));
        return dest;
    }

    private int findLineEnd(ByteBuffer buffer) {
        for (int i = buffer.position(); i < buffer.limit() - 1; i++) {
            if (buffer.get(i) == '\r' && buffer.get(i + 1) == '\n') {
                return i;
            }
        }
        return -1;
    }
}
