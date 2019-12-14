package pl.msulima.redis.benchmark.log.protocol;

import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.runner.RunWith;
import redis.clients.jedis.Protocol;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.msulima.redis.benchmark.log.protocol.DynamicEncoder.CHARSET;

@RunWith(JUnitQuickcheck.class)
public class DynamicEncoderTest {

    private static final int BUFFER_SIZE = 8 * 1024;
    private final ByteBuffer out = ByteBuffer.allocate(BUFFER_SIZE);
    private final DynamicEncoder encoder = new DynamicEncoder();

    @Property
    public void shouldTreatEmptyBufferAsCompletedWrite() {
        assertThat(encoder.write(out)).isTrue();
    }

    @Property
    public void shouldEventuallyWriteForDifferentBufferSizes() {
        // given
        encoder.setRequest(Protocol.Command.INFO, new byte[]{'a', 'b', 'c'}, new byte[]{'d', 'e', 'f'});

        // when
        out.limit(3);
        boolean completed1 = encoder.write(out);
        out.limit(100);
        boolean completed2 = encoder.write(out);

        // then
        out.flip();
        assertThat(completed1).isFalse();
        assertThat(completed2).isTrue();
        assertThat(new String(out.array(), 0, out.limit(), CHARSET)).isEqualTo("*3\r\n$4\r\nINFO\r\n$3\r\nabc\r\n$3\r\ndef\r\n");
    }

    @Property
    public void testSimpleString(@From(CommandGenerator.class) CommandTestCase test) {
        // given
        encoder.setRequest(test.command, test.args);

        // when
        boolean completed;
        do {
            out.limit(out.position() + test.bufferSize);
            completed = encoder.write(out);
        } while (!completed);

        // then
        out.flip();
        assertThat(new String(out.array(), 0, out.limit(), CHARSET)).isEqualTo(test.expected);
    }

    private static final class CommandTestCase {

        private final String expected;
        private final int bufferSize;
        private final Protocol.Command command;
        private final byte[][] args;

        private CommandTestCase(String expected, int bufferSize, Protocol.Command command, byte[][] args) {
            this.expected = expected;
            this.bufferSize = bufferSize;
            this.command = command;
            this.args = args;
        }
    }

    public static final class CommandGenerator extends Generator<CommandTestCase> {

        private static final int MAX_ARGS = 3;
        private static final int MAX_ARG_LENGTH = 10;
        private static final int MIN_BUFFER_SIZE = 16;
        private static final int MAX_BUFFER_SIZE = 128;

        public CommandGenerator() {
            super(CommandTestCase.class);
        }

        @Override
        public CommandTestCase generate(SourceOfRandomness r, GenerationStatus s) {
            StringBuilder sb = new StringBuilder();
            byte[][] args = new byte[r.nextInt(MAX_ARGS)][];

            Protocol.Command command = Protocol.Command.values()[r.nextInt(Protocol.Command.values().length)];
            sb.append("*").append(args.length + 1).append("\r\n");
            sb.append("$").append(command.toString().length()).append("\r\n").append(command.toString()).append("\r\n");

            for (int i = 0; i < args.length; i++) {
                args[i] = new byte[r.nextInt(MAX_ARG_LENGTH)];
                Arrays.fill(args[i], (byte) 'x');
                sb.append("$").append(args[i].length).append("\r\n").append("x".repeat(args[i].length)).append("\r\n");
            }
            int bufferSize = r.nextInt(MIN_BUFFER_SIZE, MAX_BUFFER_SIZE);

            return new CommandTestCase(sb.toString(), bufferSize, command, args);
        }
    }
}
