package pl.msulima.redis.benchmark.nonblocking;

import com.google.common.base.Charsets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class ProtocolReader {

    private final ByteBuffer in;
    private final byte[] lengthBuf = new byte[128];
    private static final byte[] CRLF = new byte[]{'\r', '\n'};

    private byte[] readBuf;
    private int bufOffset = 0;
    private ReaderState state = ReaderState.INITIAL;
    private int length;

    public ProtocolReader(int size) {
        this(ByteBuffer.allocate(size));
    }

    public ProtocolReader(ByteBuffer in) {
        this.in = in;
    }

    public boolean read(Response response) {
        response.clear();

        if (in.remaining() == 0) {
            return false;
        }

        boolean allRead = readInternal(response);
        if (allRead) {
            state = ReaderState.INITIAL;
        }
        return allRead;
    }

    private boolean readInternal(Response response) {
        switch (state) {
            case SIMPLE_STRING:
                return simpleString(response);
            case BULK_STRING_START:
                return bulkStringStart(response);
            case BULK_STRING_READ_RESPONSE:
                return bulkStringReadResponse(response);
            default:
                byte read = in.get();

                switch (read) {
                    case '+':
                        return simpleString(response);
                    case '$':
                        return bulkStringStart(response);
                    default:
                        throw new RuntimeException("Could not read response " + read);
                }
        }
    }

    private boolean simpleString(Response response) {
        state = ReaderState.SIMPLE_STRING;

        if (!fillBuffer()) {
            return false;
        }

        response.setSimpleString(new String(lengthBuf, 0, bufOffset, Charsets.US_ASCII));
        bufOffset = 0;
        return true;
    }

    private boolean bulkStringStart(Response response) {
        state = ReaderState.BULK_STRING_START;

        if (!readLength()) {
            return false;
        }

        return bulkStringReadResponse(response);
    }

    private boolean bulkStringReadResponse(Response response) {
        state = ReaderState.BULK_STRING_READ_RESPONSE;

        if (length == -1) {
            response.setNull(true);
            return true;
        }

        if (readBuf == null) {
            readBuf = new byte[length];
        }

        int leftToRead = length - bufOffset;
        int canRead = Math.min(leftToRead, in.remaining());

        in.get(readBuf, bufOffset, canRead);
        bufOffset += canRead;

        if (leftToRead < canRead || in.remaining() == 0) {
            return false;
        }

        if (in.get() == CRLF[0]) {
            if (in.remaining() == 0) {
                return false;
            }
            in.get();
        }

        response.setBulkString(readBuf);
        readBuf = null;
        bufOffset = 0;

        return true;
    }

    private boolean readLength() {
        if (!fillBuffer()) {
            return false;
        }

        length = 0;
        int i = 0;
        if (lengthBuf[0] == '-') {
            i++;
        }
        for (; i < bufOffset; i++) {
            length = length * 10 + (lengthBuf[i] - '0');
        }
        if (lengthBuf[0] == '-') {
            length = -length;
        }

        bufOffset = 0;
        return true;
    }

    private boolean fillBuffer() {
        byte read = 0;
        int remaining = in.remaining();

        while (remaining-- > 0 && (read = in.get()) != CRLF[1]) {
            lengthBuf[bufOffset++] = read;
        }

        if (read != '\n') {
            return false;
        }

        bufOffset--;
        return true;
    }

    int receive(SocketChannel channel) throws IOException {
        in.clear();
        int read = channel.read(in);
        in.flip();
        return read;
    }
}

enum ReaderState {
    INITIAL, BULK_STRING_START, BULK_STRING_READ_RESPONSE, SIMPLE_STRING;
}