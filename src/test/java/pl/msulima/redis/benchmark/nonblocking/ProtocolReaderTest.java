package pl.msulima.redis.benchmark.nonblocking;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProtocolReaderTest {

    public static final int BUFFER_SIZE = 64;
    private final byte[] nioBytes = new byte[BUFFER_SIZE];

    @Test
    public void readSimpleString() {
        Response response = new Response();

        int sourceBytes = writeSimpleString("OK", nioBytes, 0);
        sourceBytes += writeSimpleString("", nioBytes, sourceBytes);
        writeSimpleString("long string which doesn't fit in buffer bla bla bla bla", nioBytes, sourceBytes);

        int nextPosition = ProtocolReader.read(nioBytes, 0, response);
        assertThat(response.getString()).isEqualTo("OK");

        nextPosition = ProtocolReader.read(nioBytes, nextPosition, response);
        assertThat(response.getString()).isEqualTo("");

        assertThat(ProtocolReader.read(nioBytes, nextPosition, response)).isEqualTo(-1);
    }

    private int writeSimpleString(String string, byte[] nioBytes, int destPos) {
        byte[] source = ("+" + string + "\r\n").getBytes();
        System.arraycopy(source, 0, nioBytes, destPos, Math.min(source.length, nioBytes.length - destPos));
        return source.length;
    }

    @Test
    public void readBinaryString() {
        Response response = new Response();

        int sourceBytes = writeBulkString("OK", nioBytes, 0);
        sourceBytes += writeBulkString("", nioBytes, sourceBytes);
        writeBulkString("long string which doesn't fit in buffer bla bla", nioBytes, sourceBytes);

        int nextPosition = ProtocolReader.read(nioBytes, 0, response);
        assertThat(response.getString()).isEqualTo("OK");

        nextPosition = ProtocolReader.read(nioBytes, nextPosition, response);
        assertThat(response.getString()).isEqualTo("");

        assertThat(ProtocolReader.read(nioBytes, nextPosition, response)).isEqualTo(-1);
    }

    @Test
    public void readNull() {
        Response response = new Response();

        byte[] nullResponse = "$-1\r\n".getBytes();
        System.arraycopy(nullResponse, 0, nioBytes, 0, nullResponse.length);

        int nextPosition = ProtocolReader.read(nioBytes, 0, response);
        assertThat(nextPosition).isEqualTo(5);
        assertThat(response.isNull()).isTrue();
    }

    private int writeBulkString(String string, byte[] nioBytes, int destPos) {
        byte[] encodedString = (string + "\r\n").getBytes();
        byte[] source = ("$" + (encodedString.length - 2) + "\r\n").getBytes();

        System.arraycopy(source, 0, nioBytes, destPos, Math.min(source.length, nioBytes.length - destPos));
        System.arraycopy(encodedString, 0, nioBytes, destPos + source.length, Math.min(encodedString.length, nioBytes.length - destPos - source.length));

        return source.length + encodedString.length;
    }
}
