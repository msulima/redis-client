package pl.msulima.redis.benchmark.log.protocol;

import java.nio.ByteBuffer;

import static pl.msulima.redis.benchmark.log.protocol.DynamicEncoder.*;

public class DynamicDecoder {

    private static final int DEFAULT_READ_BUFFER_SIZE = 128;
    private static final byte[] CRLF = new byte[]{'\r', '\n'};

    public final Response response = Response.clearResponse();

    private byte[] readBuffer;
    private final byte[] lengthBuffer = new byte[MAX_INTEGER_LENGTH + CRLF.length];
    private int bufferPosition = 0;
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
        if (!in.hasRemaining()) {
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
        if (readBuffer == null) {
            readBuffer = new byte[DEFAULT_READ_BUFFER_SIZE];
            bufferPosition = 0;
        }

        if (!fillReadBuffer(in)) {
            return false;
        }

        response.setSimpleString(new String(readBuffer, 0, bufferPosition, CHARSET));
        readBuffer = null;
        bufferPosition = 0;
        return true;
    }

    private boolean fillReadBuffer(ByteBuffer in) {
        int remaining = in.remaining();
        for (int i = 0; i < remaining; i++) {
            byte read = in.get();
            if (bufferPosition == readBuffer.length) {
                byte[] bytes = new byte[bufferPosition + DEFAULT_READ_BUFFER_SIZE];
                System.arraycopy(readBuffer, 0, bytes, 0, readBuffer.length);
                readBuffer = bytes;
            }
            readBuffer[bufferPosition++] = read;
            if (read == CRLF[1]) {
                bufferPosition -= CRLF.length;
                return true;
            }
        }
        return false;
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

        if (readBuffer == null) {
            readBuffer = new byte[length];
        }

        int leftToRead = length - bufferPosition;
        int canRead = Math.min(leftToRead, in.remaining());

        in.get(readBuffer, bufferPosition, canRead);
        bufferPosition += canRead;

        if (leftToRead < canRead || !in.hasRemaining()) {
            return false;
        }

        if (in.get() == CRLF[0]) {
            if (!in.hasRemaining()) {
                return false;
            }
            in.get();
        }

        if (array != null) {
            array[arrayIdx++] = readBuffer;
            readBuffer = null;
            bufferPosition = 0;
            if (arrayIdx < array.length) {
                state = DecoderState.INITIAL;
                return readInternal(in);
            } else {
                response.setArray(array);
                array = null;
                arrayIdx = 0;
            }
        } else {
            response.setBulkString(readBuffer);
            readBuffer = null;
            bufferPosition = 0;
        }
        return true;
    }

    private boolean readLength(ByteBuffer in) {
        if (!fillLengthBuffer(in)) {
            return false;
        }

        length = 0;
        int i = 0;
        if (lengthBuffer[0] == '-') {
            i++;
        }
        for (; i < bufferPosition; i++) {
            length = length * 10 + (lengthBuffer[i] - '0');
        }
        if (lengthBuffer[0] == '-') {
            length = -length;
        }

        bufferPosition = 0;
        return true;
    }

    private boolean fillLengthBuffer(ByteBuffer in) {
        int remaining = in.remaining();
        for (int i = 0; i < remaining; i++) {
            byte read = in.get();
            lengthBuffer[bufferPosition++] = read;
            if (read == CRLF[1]) {
                bufferPosition -= CRLF.length;
                return true;
            }
        }
        return false;
    }
}
