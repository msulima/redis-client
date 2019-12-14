package pl.msulima.redis.benchmark.log.logbuffer;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PublicationImageTest {

    @Test
    public void shouldReserveData() {
        // given
        PublicationImage publicationImage = new PublicationImage(128);
        int bytesToWrite = 8;
        BufferClaim firstClaim = new BufferClaim();
        BufferClaim secondClaim = new BufferClaim();

        // when
        publicationImage.writeClaim(bytesToWrite, firstClaim);
        publicationImage.writeClaim(bytesToWrite, secondClaim);

        // then
        assertThat(firstClaim.getByteBuffer().remaining()).isEqualTo(bytesToWrite);
        assertThat(secondClaim.getByteBuffer().remaining()).isEqualTo(bytesToWrite);
        assertThat(publicationImage.tail()).isEqualTo(0);

        // when
        secondClaim.commit();

        // then
        assertThat(publicationImage.remaining()).isEqualTo(0);

        // when
        firstClaim.commit();

        // then
        assertThat(publicationImage.remaining()).isEqualTo(bytesToWrite * 2);
    }
}
