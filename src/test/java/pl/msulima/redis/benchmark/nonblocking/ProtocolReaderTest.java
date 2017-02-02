package pl.msulima.redis.benchmark.nonblocking;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProtocolReaderTest {

    public static final int BUFFER_SIZE = 64;

    @Test
    public void readSimpleString() {
        Response response = new Response();

        byte[] nioBytes = new byte[BUFFER_SIZE];

        int sourceBytes = writeSimpleString("OK", nioBytes, 0);
        sourceBytes += writeSimpleString("", nioBytes, sourceBytes);
        writeSimpleString("long string which doesn't fit in buffer bla bla bla bla", nioBytes, sourceBytes);

        int nextPosition = ProtocolReader.read(nioBytes, 0, response);
        assertThat(response.getSimpleString()).isEqualTo("OK");

        nextPosition = ProtocolReader.read(nioBytes, nextPosition, response);
        assertThat(response.getSimpleString()).isEqualTo("");

        assertThat(ProtocolReader.read(nioBytes, nextPosition, response)).isEqualTo(-1);
    }

    private int writeSimpleString(String string, byte[] nioBytes, int destPos) {
        byte[] source = ("+" + string + "\r\n").getBytes();
        System.arraycopy(source, 0, nioBytes, destPos, Math.min(source.length, nioBytes.length - destPos));
        return source.length;
    }
}
