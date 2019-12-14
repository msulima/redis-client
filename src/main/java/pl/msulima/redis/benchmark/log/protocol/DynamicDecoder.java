package pl.msulima.redis.benchmark.log.protocol;

import java.nio.ByteBuffer;

import static pl.msulima.redis.benchmark.log.protocol.DynamicEncoder.*;

public class DynamicDecoder {

    private final byte[] lengthBuf = new byte[128];
    private static final byte[] CRLF = new byte[]{'\r', '\n'};

    public final Response response = Response.clearResponse();

    private byte[] readBuf;
    private int bufOffset = 0;
    private DecoderState state = DecoderState.INITIAL;
    private int length;
    private byte[][] array;
    private int arrayIdx;

    public boolean read(ByteBuffer in) {
        response.clear();

        boolean allRead = readInternal(in);
        if (allRead) {
            state = DecoderState.INITIAL;
        }
        return allRead;
    }

    private boolean readInternal(ByteBuffer in) {
        if (in.remaining() == 0) {
            return false;
        }

        switch (state) {
            case SIMPLE_STRING:
                return simpleString(in);
            case ARRAY_START:
                return arrayStart(in);
            case BULK_STRING_START:
                return bulkStringStart(in);
            case BULK_STRING_READ_RESPONSE:
                return bulkStringReadResponse(in);
            default:
                byte read = in.get();

                switch (read) {
                    case '+':
                        return simpleString(in);
                    case ASTERISK:
                        return arrayStart(in);
                    case DOLLAR:
                        return bulkStringStart(in);
                    default:
                        throw new RuntimeException("Could not read response " + read);
                }
        }
    }

    private boolean simpleString(ByteBuffer in) {
        state = DecoderState.SIMPLE_STRING;

        if (!fillBuffer(in)) {
            return false;
        }

        // TODO what if simple string is longer than lengthBuf.length?
        response.setSimpleString(new String(lengthBuf, 0, bufOffset, CHARSET));
        bufOffset = 0;
        return true;
    }

    private boolean arrayStart(ByteBuffer in) {
        state = DecoderState.ARRAY_START;

        if (!readLength(in)) {
            return false;
        }
        array = new byte[length][];
        state = DecoderState.INITIAL;
        return readInternal(in);
    }

    private boolean bulkStringStart(ByteBuffer in) {
        state = DecoderState.BULK_STRING_START;

        if (!readLength(in)) {
            return false;
        }

        return bulkStringReadResponse(in);
    }

    private boolean bulkStringReadResponse(ByteBuffer in) {
        state = DecoderState.BULK_STRING_READ_RESPONSE;

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

        if (array != null) {
            array[arrayIdx++] = readBuf;
            readBuf = null;
            bufOffset = 0;
            if (arrayIdx < array.length) {
                state = DecoderState.INITIAL;
                return readInternal(in);
            } else {
                response.setArray(array);
                array = null;
                arrayIdx = 0;
            }
        } else {
            response.setBulkString(readBuf);
            readBuf = null;
            bufOffset = 0;
        }
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
