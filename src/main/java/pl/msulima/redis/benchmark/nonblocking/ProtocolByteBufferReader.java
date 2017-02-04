package pl.msulima.redis.benchmark.nonblocking;

import com.google.common.base.Charsets;

import java.nio.ByteBuffer;

public class ProtocolByteBufferReader {

    private final ByteBuffer in;
    private final byte[] lengthBuf = new byte[64];
    private byte[] readBuf;
    private int bufOffset = 0;
    private State state = State.INITIAL;
    private int length;

    public ProtocolByteBufferReader(ByteBuffer in) {
        this.in = in;
    }

    public boolean read(Response response) {
        response.clear();

        boolean allRead = readInternal(response);
        if (allRead) {
            state = State.INITIAL;
        }
        return allRead;
    }

    private boolean readInternal(Response response) {
        switch (state) {
            case BULK_STRING_START:
                return bulkStringStart(response);
            case BULK_STRING_READ_RESPONSE:
                return bulkStringReadResponse(response);
            default:
                if (in.remaining() == 0) {
                    return false;
                }

                int i;
                byte read = in.get();

                switch (read) {
                    case '+':
                        int remaining = in.remaining();

                        for (i = 0; i < remaining; i++) {
                            read = in.get();
                            if (read != '\r') {
                                lengthBuf[i] = read;
                            } else {
                                break;
                            }
                        }
                        in.position(in.position() + 1);

                        response.setString(new String(lengthBuf, 0, i, Charsets.US_ASCII));
                        break;
                    case '$':
                        return bulkStringStart(response);
                    default:
                        throw new RuntimeException("Could not read response " + read);
                }
                return true;
        }
    }

    private boolean bulkStringStart(Response response) {
        state = State.BULK_STRING_START;

        if (!readLength()) {
            return false;
        }

        return bulkStringReadResponse(response);
    }

    private boolean bulkStringReadResponse(Response response) {
        state = State.BULK_STRING_READ_RESPONSE;

        if (length == -1) {
            response.setIsNull(true);
            return true;
        }

        if (readBuf == null) {
            readBuf = new byte[length];
        }

        int leftToRead = length - bufOffset;
        int canRead = Math.min(leftToRead, in.remaining());

        in.get(readBuf, bufOffset, canRead);
        bufOffset += canRead;

        if (leftToRead < canRead) {
            return false;
        }

        if (in.remaining() == 0) {
            return false;
        }
        if (in.get() == '\r') {
            if (in.remaining() == 0) {
                return false;
            }
            in.get();
        }

        response.setString(new String(readBuf, 0, length, Charsets.US_ASCII));
        readBuf = null;
        bufOffset = 0;

        return true;
    }

    private boolean readLength() {
        byte read = 0;
        int remaining = in.remaining();

        while (remaining-- > 0 && (read = in.get()) != '\n') {
            lengthBuf[bufOffset++] = read;
        }

        if (read != '\n') {
            return false;
        }

        length = 0;

        int start = 0;
        if (lengthBuf[0] == '-') {
            length = -1;
            start = 1;
        }

        for (int i = start; i < bufOffset - 1; i++) {
            length = length * 10 + (lengthBuf[i] - '0');
        }

        bufOffset = 0;
        return true;
    }
}

enum State {
    INITIAL, BULK_STRING_START, BULK_STRING_READ_RESPONSE;
}