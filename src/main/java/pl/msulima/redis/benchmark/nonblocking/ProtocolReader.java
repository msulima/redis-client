package pl.msulima.redis.benchmark.nonblocking;

public class ProtocolReader {

    public static final int CRLF = 2;

    public static int read(final byte[] in, final int offset, final Response response) {
        response.clear();
        int pos = offset;

        if (pos + 16 > in.length) {
            return -1;
        }

        byte responseType = in[pos++];

        switch (responseType) {
            case '+':
                while (pos < in.length && in[pos] != '\r') {
                    pos++;
                }
                if (pos == in.length) {
                    return -1;
                }
                int stringStart = offset + 1;
                response.setString(new String(in, stringStart, pos - (stringStart)));
                pos += CRLF;
                break;
            case '$':
                int length = 0;
                if (in[pos] == '-') {
                    response.setIsNull(true);
                    return pos + 4;
                }

                while (pos < in.length && in[pos] != '\r') {
                    length = length * 10 + (in[pos] - '0');
                    pos++;
                }
                if (pos + CRLF + length + CRLF >= in.length) {
                    return -1;
                }

                response.setString(new String(in, pos + CRLF, length));
                pos += CRLF + length + CRLF;
                break;
            default:
                throw new RuntimeException("Could not read response");
        }

        return pos;
    }
}
