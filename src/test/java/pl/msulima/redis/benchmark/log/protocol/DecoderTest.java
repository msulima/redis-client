package pl.msulima.redis.benchmark.log.protocol;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class DecoderTest {

    @Test
    public void testSimpleString() {
        // given
        String ok = "OK";
        byte[] input = ("+" + ok + "\r\n").getBytes(StandardCharsets.US_ASCII);

        // when
        Response response = Decoder.read(input);

        // then
        assertThat(response).isEqualTo(Response.simpleString(ok));
    }

    @Test
    public void testBulkString() {
        // given
        String ok = "OK";
        byte[] input = ("$2\r\n" + ok + "\r\n").getBytes(StandardCharsets.US_ASCII);

        // when
        Response response = Decoder.read(input);

        // then
        assertThat(response).isEqualTo(Response.bulkString(ok.getBytes()));
    }

    @Test
    public void testNull() {
        // given
        byte[] input = "$-1\r\n".getBytes(StandardCharsets.US_ASCII);

        // when
        Response response = Decoder.read(input);

        // then
        assertThat(response).isEqualTo(Response.nullResponse());
    }
}
