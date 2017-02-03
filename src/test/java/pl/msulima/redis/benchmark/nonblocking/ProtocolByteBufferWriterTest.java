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
        final int messagesToWrite = 20;
        byte[] fullOutputBytes = new byte[messagesToWrite * 64];
        int bytesRead = 0;
        ByteBuffer out = ByteBuffer.allocate(64);
        ProtocolByteBufferWriter reader = new ProtocolByteBufferWriter(out);

        // when
        int messagesWritten = 0;

        byte[][] args = new byte[][]{"1".getBytes(), new byte[1]};

        while (messagesWritten < messagesToWrite) {
            while (messagesWritten < messagesToWrite && reader.write(Protocol.Command.SET, args)) {
                messagesWritten++;
            }

            out.flip();
            out.get(fullOutputBytes, bytesRead, out.limit());
            bytesRead += out.limit();
            out.clear();
        }

        // then
        String expected = "*3\r\n$3\r\nSET\r\n$1\r\n1\r\n$1\r\n\0\r\n";
        assertThat(new String(fullOutputBytes, 0, bytesRead, Charsets.US_ASCII))
                .isEqualTo(String.join("", Collections.nCopies(messagesToWrite, expected)));
    }
}
