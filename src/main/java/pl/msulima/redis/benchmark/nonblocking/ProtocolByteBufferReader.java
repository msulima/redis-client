package pl.msulima.redis.benchmark.nonblocking;

import com.google.common.base.Charsets;

import java.nio.ByteBuffer;

public class ProtocolByteBufferReader {

    private final ByteBuffer in;
    private final byte[] buf = new byte[64];

    public ProtocolByteBufferReader(ByteBuffer in) {
        this.in = in;
    }

    public boolean read(Response response) {
        response.clear();

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
                        buf[i] = read;
                    } else {
                        break;
                    }
                }
                in.position(in.position() + 1);

                response.setString(new String(buf, 0, i, Charsets.US_ASCII));
                break;
            case '$':
                read = in.get();
                if (read == '-') {
                    response.setIsNull(true);
                    in.position(in.position() + 3);
                    return false;
                }

                int length = 0;
                do {
                    length = length * 10 + (read - '0');
                } while ((read = in.get()) != '\r');

                in.get();
                in.get(buf, 0, length);
                in.position(in.position() + 2);

                response.setString(new String(buf, 0, length, Charsets.US_ASCII));
                break;
            default:
                throw new RuntimeException("Could not read response " + read);
        }
        return true;
    }
}
