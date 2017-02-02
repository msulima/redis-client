package pl.msulima.redis.benchmark.nonblocking;

public class ProtocolReader {

    public static int read(final byte[] in, final int offset, final Response response) {
        response.clear();
        int pos = offset;

        if (pos + 2 > in.length) {
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
                String simpleString = new String(in, stringStart, pos - (stringStart));
                response.setSimpleString(simpleString);
                pos += 2;
        }

        return pos;
    }
}
