package pl.msulima.redis.benchmark.nonblocking;

import com.google.common.base.Charsets;
import org.junit.Test;
import redis.clients.jedis.Protocol;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class ProtocolByteBufferWriterTest {

    @Test
    public void testSingle() {
        // given
        ByteBuffer out = ByteBuffer.allocate(64);
        ProtocolByteBufferWriter reader = new ProtocolByteBufferWriter(out);

        // when
        reader.write(Protocol.Command.SET, "1".getBytes(), "".getBytes());

        // then
        out.flip();
        byte[] fullOutputBytes = new byte[64];
        out.get(fullOutputBytes, 0, out.limit());

        String expected = "*3\r\n$3\r\nSET\r\n$1\r\n1\r\n$0\r\n\r\n";
        assertThat(new String(fullOutputBytes, 0, out.limit(), Charsets.US_ASCII)).isEqualTo(expected);
    }

    @Test
    public void testMultiple() {
        // given
        final int messagesToWrite = 7;
        int bufferSize = 64;
        byte[] fullOutputBytes = new byte[messagesToWrite * bufferSize];
        int bytesRead = 0;
        ByteBuffer out = ByteBuffer.allocate(bufferSize);
        ProtocolByteBufferWriter reader = new ProtocolByteBufferWriter(out);

        // when
        int messagesWritten = 0;
        int loops = 0;

        byte[] emptyBytes = new byte[33];
        byte[][] args = new byte[][]{"1".getBytes(), emptyBytes};

        while (messagesWritten < messagesToWrite && loops < messagesToWrite * 2) {
            while (messagesWritten < messagesToWrite && reader.write(Protocol.Command.SET, args)) {
                messagesWritten++;
            }

            out.flip();
            out.get(fullOutputBytes, bytesRead, out.limit());
            bytesRead += out.limit();
            out.clear();

            loops++;
        }

        // then
        assertThat(loops).isLessThan(messagesToWrite * 2);

        String expected = "*3\r\n$3\r\nSET\r\n$1\r\n1\r\n$" + emptyBytes.length + "\r\n" + new String(emptyBytes, Charsets.US_ASCII) + "\r\n";
        assertThat(new String(fullOutputBytes, 0, bytesRead, Charsets.US_ASCII))
                .isEqualTo(String.join("", Collections.nCopies(messagesToWrite, expected)));
    }
}
