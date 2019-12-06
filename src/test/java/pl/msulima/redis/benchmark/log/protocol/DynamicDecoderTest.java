package pl.msulima.redis.benchmark.log.protocol;

import com.google.common.base.Charsets;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DynamicDecoderTest {

    private static final int BUFFER_SIZE = 64;
    private final DynamicDecoder reader = new DynamicDecoder();

    @Test
    public void testSimpleString() {
        // given
        String ok = "OK";
        ByteBuffer in = createByteBuffer("+" + ok + "\r\n");

        // when
        reader.read(in);

        // then
        assertThat(reader.response).isEqualTo(Response.simpleString(ok));
    }

    @Test
    public void testBulkString() {
        // given
        String ok = "OK";
        ByteBuffer in = createByteBuffer("$2\r\n" + ok + "\r\n");

        // when
        reader.read(in);

        // then
        assertThat(reader.response).isEqualTo(Response.bulkString(ok.getBytes()));
    }

    @Test
    public void testNull() {
        // given
        ByteBuffer in = createByteBuffer("$-1\r\n");

        // when
        reader.read(in);

        // then
        assertThat(reader.response).isEqualTo(Response.nullResponse());
    }

    private ByteBuffer createByteBuffer(String s) {
        ByteBuffer in = ByteBuffer.allocate(BUFFER_SIZE);
        in.put(s.getBytes(StandardCharsets.UTF_8));
        in.flip();
        return in;
    }

    @Test
    public void testMultiple() {
        runTest(50, 3, 2);
        runTest(100, BUFFER_SIZE, 10);
        runTest(100, 3, BUFFER_SIZE + 3);
    }

    private void runTest(int messagesToWrite, int stepSize, int emptyBytesLength) {
        byte[] bytes = new byte[emptyBytesLength];
        String ok = new String(bytes, Charsets.US_ASCII);
        ByteBuffer fullInput = ByteBuffer.allocate(messagesToWrite * 2 * (emptyBytesLength + 10));
        for (int i = 0; i < messagesToWrite; i++) {
            fullInput.put(("+" + ok + "\r\n").getBytes());
            fullInput.put(("$" + emptyBytesLength + "\r\n" + ok + "\r\n").getBytes());
        }
        fullInput.flip();

        // when
        int maxLoops = fullInput.remaining() / stepSize + 3;

        List<Response> responses = new ArrayList<>();
        int limit = fullInput.limit();

        ByteBuffer in = ByteBuffer.allocate(BUFFER_SIZE);
        for (int loops = 0; loops < maxLoops; loops++) {
            in.clear();
            in.put(fullInput.limit(Math.min(limit, fullInput.position() + stepSize)));
            in.flip();

            while (reader.read(in)) {
                pl.msulima.redis.benchmark.log.protocol.Response copy = reader.response.copy();
                responses.add(copy);
            }
        }

        // then
        List<Response> expectedResponses = new ArrayList<>();
        for (int i = 0; i < messagesToWrite; i++) {
            expectedResponses.add(Response.simpleString(ok));
            expectedResponses.add(Response.bulkString(bytes));
        }
        assertThat(responses).isEqualTo(expectedResponses);
    }
}
