package pl.msulima.redis.benchmark.log.protocol;

import com.google.common.base.Charsets;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitQuickcheck.class)
public class DynamicDecoderTest {

    private static final int BUFFER_SIZE = 64;
    private final DynamicDecoder reader = new DynamicDecoder();

    @Property
    public void testSimpleString(@From(SimpleStringGenerator.class) String text) {
        // given
        ByteBuffer in = createByteBuffer("+" + text + "\r\n");

        // when
        reader.read(in);

        // then
        assertThat(reader.response).isEqualTo(Response.simpleString(text));
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

    public static class SimpleStringGenerator extends Generator<String> {

        private static final String LOWERCASE_CHARS = "abcdefghijklmnopqrstuvwxyz";
        private static final String UPPERCASE_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        private static final String NUMBERS = "0123456789";
        private static final String SPECIAL_CHARS = ".-\\;:_@[]^/|}{";
        private static final String ALL_MY_CHARS = LOWERCASE_CHARS + UPPERCASE_CHARS + NUMBERS + SPECIAL_CHARS;
        private static final int CAPACITY = BUFFER_SIZE - 3;

        public SimpleStringGenerator() {
            super(String.class);
        }

        @Override
        public String generate(SourceOfRandomness r, GenerationStatus s) {
            int capacity = r.nextInt(CAPACITY - 1) + 1;
            StringBuilder sb = new StringBuilder(capacity);
            for (int i = 0; i < capacity; i++) {
                int randomIndex = r.nextInt(ALL_MY_CHARS.length());
                sb.append(ALL_MY_CHARS.charAt(randomIndex));
            }
            return sb.toString();
        }
    }
}
