package pl.msulima.redis.benchmark.log.protocol;

import org.junit.Test;
import redis.clients.jedis.Protocol;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class EncoderTest {

    @Test
    public void testSimpleString() {
        // given & when
        byte[] output = Encoder.write(Protocol.Command.SET, "1".getBytes(StandardCharsets.US_ASCII), "".getBytes(StandardCharsets.US_ASCII));

        // then
        String expected = "*3\r\n$3\r\nSET\r\n$1\r\n1\r\n$0\r\n\r\n";
        assertThat(new String(output, StandardCharsets.US_ASCII)).isEqualTo(expected);
    }
}
