package pl.msulima.redis.benchmark.nonblocking;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class ProtocolByteBufferReaderTest {

    private static final int BUFFER_SIZE = 64;
    private final ByteBuffer in = ByteBuffer.allocate(BUFFER_SIZE);
    private final ProtocolByteBufferReader reader = new ProtocolByteBufferReader(in);

    @Test
    public void testSimpleString() {
        // given
        String ok = "OK";
        in.put(("+" + ok + "\r\n").getBytes());
        in.flip();

        // when
        Response response = new Response();
        reader.read(response);

        // then
        assertThat(response.getString()).isEqualTo(ok);
    }

    @Test
    public void testBulkString() {
        // given
        String ok = "OK";
        in.put(("$2\r\n" + ok + "\r\n").getBytes());
        in.flip();

        // when
        Response response = new Response();
        reader.read(response);

        // then
        assertThat(response.getString()).isEqualTo(ok);
    }

//    @Test
//    public void testMultiple() {
//        runTest(89);
//    }
//
//    @Test
//    public void testLong() {
//        runTest(512);
//    }
//
//    @Test
//    public void testMultipleInLoop() {
//        for (int i = 0; i < 100; i++) {
//            try {
//                runTest(i);
//            } catch (RuntimeException | AssertionError ex) {
//                System.err.println("Exception in iteration " + i);
//                throw new RuntimeException(ex);
//            }
//        }
//    }
//
//    private void runTest(int emptyBytesLength) {
//        // given
//        final int messagesToWrite = 100;
//
//        final byte[] emptyBytes = new byte[emptyBytesLength];
//
//        final String expected = "*3\r\n$3\r\nSET\r\n$1\r\n1\r\n$" + emptyBytes.length + "\r\n" + new String(emptyBytes, Charsets.US_ASCII) + "\r\n";
//        final String join = String.join("", Collections.nCopies(messagesToWrite, expected));
//        final byte[] fullOutputBytes = new byte[join.getBytes().length * 2];
//
//        // when
//        final ProtocolByteBufferWriter reader = new ProtocolByteBufferWriter(out);
//        final int maxLoops = ((emptyBytesLength / BUFFER_SIZE) + 2) * messagesToWrite;
//
//        int messagesWritten = 0;
//        int loops = 0;
//        int bytesRead = 0;
//
//        while (messagesWritten < messagesToWrite && loops < maxLoops) {
//            while (messagesWritten < messagesToWrite && reader.write(Protocol.Command.SET, "1".getBytes(), emptyBytes)) {
//                messagesWritten++;
//            }
//
//            out.flip();
//            out.get(fullOutputBytes, bytesRead, out.limit());
//            bytesRead += out.limit();
//            out.clear();
//
//            loops++;
//        }
//
//        // then
//        assertThat(loops).isLessThan(maxLoops);
//        assertThat(new String(fullOutputBytes, 0, bytesRead, Charsets.US_ASCII)).isEqualTo(join);
//    }
}
