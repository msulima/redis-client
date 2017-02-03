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
        runTest(89);
    }

    @Test
    public void testLong() {
        runTest(512);
    }

    @Test
    public void testMultipleInLoop() {
        for (int i = 0; i < 100; i++) {
            try {
                runTest(i);
            } catch (RuntimeException | AssertionError ex) {
                System.err.println("Exception in iteration " + i);
                throw new RuntimeException(ex);
            }
        }
    }

    private void runTest(int emptyBytesLength) {
        // given
        final int bufferSize = 64;
        final ByteBuffer out = ByteBuffer.allocate(bufferSize);
        final int messagesToWrite = 100;

        final byte[] emptyBytes = new byte[emptyBytesLength];

        final String expected = "*3\r\n$3\r\nSET\r\n$1\r\n1\r\n$" + emptyBytes.length + "\r\n" + new String(emptyBytes, Charsets.US_ASCII) + "\r\n";
        final String join = String.join("", Collections.nCopies(messagesToWrite, expected));
        final byte[] fullOutputBytes = new byte[join.getBytes().length * 2];

        // when
        final ProtocolByteBufferWriter reader = new ProtocolByteBufferWriter(out);
        final int maxLoops = ((emptyBytesLength / bufferSize) + 2) * messagesToWrite;

        int messagesWritten = 0;
        int loops = 0;
        int bytesRead = 0;

        while (messagesWritten < messagesToWrite && loops < maxLoops) {
            while (messagesWritten < messagesToWrite && reader.write(Protocol.Command.SET, "1".getBytes(), emptyBytes)) {
                messagesWritten++;
            }

            out.flip();
            out.get(fullOutputBytes, bytesRead, out.limit());
            bytesRead += out.limit();
            out.clear();

            loops++;
        }

        // then
        assertThat(loops).isLessThan(maxLoops);
        assertThat(new String(fullOutputBytes, 0, bytesRead, Charsets.US_ASCII)).isEqualTo(join);
    }
}
