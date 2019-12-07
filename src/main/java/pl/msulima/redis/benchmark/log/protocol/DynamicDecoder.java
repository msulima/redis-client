package pl.msulima.redis.benchmark.log.protocol;

import com.google.common.base.Charsets;

import java.nio.ByteBuffer;

public class DynamicDecoder {

    private final byte[] lengthBuf = new byte[128];
    private static final byte[] CRLF = new byte[]{'\r', '\n'};

    public Response response = Response.clearResponse();

    private byte[] readBuf;
    private int bufOffset = 0;
    private ReaderState state = ReaderState.INITIAL;
    private int length;

    public boolean read(ByteBuffer in) {
        response.clear();
        if (in.remaining() == 0) {
            return false;
        }

        boolean allRead = readInternal(in);
        if (allRead) {
            state = ReaderState.INITIAL;
        }
        return allRead;
    }

    private boolean readInternal(ByteBuffer in) {
        switch (state) {
            case SIMPLE_STRING:
                return simpleString(in);
            case BULK_STRING_START:
                return bulkStringStart(in);
            case BULK_STRING_READ_RESPONSE:
                return bulkStringReadResponse(in);
            default:
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
    }

    private boolean simpleString(ByteBuffer in) {
        state = ReaderState.SIMPLE_STRING;

        if (!fillBuffer(in)) {
            return false;
        }

        response.setSimpleString(new String(lengthBuf, 0, bufOffset, Charsets.US_ASCII));
        bufOffset = 0;
        return true;
    }

    private boolean bulkStringStart(ByteBuffer in) {
        state = ReaderState.BULK_STRING_START;

        if (!readLength(in)) {
            return false;
        }

        return bulkStringReadResponse(in);
    }

    private boolean bulkStringReadResponse(ByteBuffer in) {
        state = ReaderState.BULK_STRING_READ_RESPONSE;

        if (length == -1) {
            response.setNull();
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

    private boolean readLength(ByteBuffer in) {
        if (!fillBuffer(in)) {
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

    private boolean fillBuffer(ByteBuffer in) {
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
}

enum ReaderState {
    INITIAL, BULK_STRING_START, BULK_STRING_READ_RESPONSE, SIMPLE_STRING;
}