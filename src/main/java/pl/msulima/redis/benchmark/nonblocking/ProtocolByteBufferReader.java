package pl.msulima.redis.benchmark.nonblocking;

import com.google.common.base.Charsets;

import java.nio.ByteBuffer;

public class ProtocolByteBufferReader {

    private final ByteBuffer in;
    private final byte[] buf = new byte[64];

    public ProtocolByteBufferReader(ByteBuffer in) {
        this.in = in;
    }

    public void read(Response response) {
        response.clear();

        byte responseType = in.get();

        switch (responseType) {
            case '+':
                int remaining = in.remaining();
                byte read;
                int i;

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
        }
    }
}
