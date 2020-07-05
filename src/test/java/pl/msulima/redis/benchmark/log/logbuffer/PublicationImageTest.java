package pl.msulima.redis.benchmark.log.logbuffer;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class PublicationImageTest {

    private static final int IMAGE_SIZE = 128;
    private static final int INT_SIZE = 4;
    private static final int MARGIN = 8;

    @Test
    public void shouldReturnEmptyReadClaim() {
        // given
        PublicationImage image = new PublicationImage(IMAGE_SIZE);

        // when
        ByteBuffer firstReadClaim = image.readClaim();

        // then
        assertThat(firstReadClaim.remaining()).isEqualTo(0);
    }

    @Test
    public void shouldReturnEmptyWriteClaim() {
        // given
        PublicationImage image = new PublicationImage(IMAGE_SIZE);

        // when & then
        assertThat(write(image, IMAGE_SIZE / 2)).isEqualTo(IMAGE_SIZE / 2);
        assertThat(read(image, IMAGE_SIZE / 2)).isEqualTo(IMAGE_SIZE / 2);
        assertThat(write(image, IMAGE_SIZE)).isEqualTo(IMAGE_SIZE / 2);
        assertThat(write(image, IMAGE_SIZE)).isEqualTo(IMAGE_SIZE / 2);
        assertThat(write(image, IMAGE_SIZE / 2)).isEqualTo(0);
        assertThat(write(image, IMAGE_SIZE / 2)).isEqualTo(0);
    }

    @Test
    public void shouldReserveData() {
        // given
        PublicationImage image = new PublicationImage(IMAGE_SIZE);
        int bytesToWrite = 8;

        // when
        ByteBuffer firstWrite = image.writeClaim();
        firstWrite.put(new byte[bytesToWrite]);
        image.commitWrite(firstWrite);
        ByteBuffer firstRead = image.readClaim();

        // then
        assertThat(firstWrite.remaining()).isEqualTo(IMAGE_SIZE - bytesToWrite);
        assertThat(firstRead.remaining()).isEqualTo(bytesToWrite);

        // when
        firstRead.getInt();
        image.commitRead(firstRead);
        ByteBuffer secondWrite = image.writeClaim();
        ByteBuffer secondRead = image.readClaim();

        // then
        assertThat(secondWrite.remaining()).isEqualTo(IMAGE_SIZE - bytesToWrite);
        assertThat(secondRead.remaining()).isEqualTo(bytesToWrite - INT_SIZE);
    }

    @Test
    public void shouldWrapAroundBuffer() {
        // given
        PublicationImage image = new PublicationImage(IMAGE_SIZE);

        // when & then
        assertThat(write(image, IMAGE_SIZE * 3 / 4)).isEqualTo(IMAGE_SIZE * 3 / 4);
        assertThat(read(image)).isEqualTo(IMAGE_SIZE * 3 / 4);

        assertThat(write(image, IMAGE_SIZE * 2)).isEqualTo(IMAGE_SIZE / 4);
        assertThat(read(image)).isEqualTo(IMAGE_SIZE / 4);

        assertThat(write(image, IMAGE_SIZE * 2)).isEqualTo(IMAGE_SIZE);
        assertThat(read(image)).isEqualTo(IMAGE_SIZE);
    }

    @Test
    public void shouldUseSpaceAtStartOfBuffer() {
        // given
        PublicationImage image = new PublicationImage(IMAGE_SIZE);

        // when & then
        assertThat(write(image, IMAGE_SIZE * 3 / 4)).isEqualTo(IMAGE_SIZE * 3 / 4);
        assertThat(read(image, IMAGE_SIZE / 2)).isEqualTo(IMAGE_SIZE / 2);
        assertThat(write(image, IMAGE_SIZE)).isEqualTo(IMAGE_SIZE / 4);
        assertThat(write(image, IMAGE_SIZE)).isEqualTo(IMAGE_SIZE / 2);
        assertThat(write(image, IMAGE_SIZE)).isEqualTo(0);
    }

    @Test
    public void shouldClaimAtLeastMarginBytes() {
        // given
        PublicationImage image = new PublicationImage(IMAGE_SIZE, MARGIN);

        // when & then
        assertThat(write(image, IMAGE_SIZE - MARGIN + 1)).isEqualTo(IMAGE_SIZE - MARGIN + 1);
        assertThat(write(image, IMAGE_SIZE - MARGIN)).isEqualTo(0);
        assertThat(read(image, IMAGE_SIZE / 2)).isEqualTo(IMAGE_SIZE / 2);
        assertThat(write(image, IMAGE_SIZE - MARGIN)).isEqualTo(MARGIN);
    }

    private int write(PublicationImage image, int bytesToWrite) {
        ByteBuffer buffer = image.writeClaim();
        int written = Math.min(buffer.remaining(), bytesToWrite);
        buffer.put(new byte[written]);
        image.commitWrite(buffer);
        return written;
    }

    private int read(PublicationImage image) {
        return read(image, IMAGE_SIZE);
    }

    private int read(PublicationImage image, int imageSize) {
        ByteBuffer buffer = image.readClaim();
        int remaining = Math.min(buffer.remaining(), imageSize);
        buffer.position(buffer.position() + remaining);
        image.commitRead(buffer);
        return remaining;
    }
}
