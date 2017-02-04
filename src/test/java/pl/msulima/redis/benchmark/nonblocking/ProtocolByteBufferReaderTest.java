package pl.msulima.redis.benchmark.nonblocking;

import com.google.common.base.Charsets;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
        assertThat(response).isEqualTo(Response.simpleString(ok));
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
        assertThat(response).isEqualTo(Response.bulkString(ok.getBytes()));
    }

    @Test
    public void testNull() {
        // given
        in.put(("$-1\r\n").getBytes());
        in.flip();

        // when
        Response response = new Response();
        reader.read(response);

        // then
        assertThat(response).isEqualTo(Response.nullResponse());
    }

    @Test
    public void testMultiple() {
        runTest(100, 3, 2);
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
        Response response = new Response();
        int limit = fullInput.limit();

        for (int loops = 0; loops < maxLoops; loops++) {
            in.clear();
            in.put((ByteBuffer) fullInput.limit(Math.min(limit, fullInput.position() + stepSize)));
            in.flip();

            while (reader.read(response)) {
                responses.add(response.copy());
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
