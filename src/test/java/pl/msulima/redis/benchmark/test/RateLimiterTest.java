package pl.msulima.redis.benchmark.test;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RateLimiterTest {

    @Test
    public void shouldCompensateForPassingTime() {
        // given
        int secondRate = 2000;
        Clock clock = mock(Clock.class);
        when(clock.currentRate()).thenReturn(secondRate);
        RateLimiter rateLimiter = new RateLimiter(clock, 1);

        // when & then
        assertThat(rateLimiter.calculateLimit()).isEqualTo(0);
        when(clock.microTime()).thenReturn(500L);
        assertThat(rateLimiter.calculateLimit()).isEqualTo(1);
        when(clock.microTime()).thenReturn(1100L);
        assertThat(rateLimiter.calculateLimit()).isEqualTo(1);
        when(clock.microTime()).thenReturn(2000L);
        assertThat(rateLimiter.calculateLimit()).isEqualTo(2);
    }

    @Test
    public void shouldRoundToBatchSize() {
        // given
        int secondRate = 2000;
        Clock clock = mock(Clock.class);
        when(clock.currentRate()).thenReturn(secondRate);
        RateLimiter rateLimiter = new RateLimiter(clock, 10);

        // when & then
        assertThat(rateLimiter.calculateLimit()).isEqualTo(0);
        when(clock.microTime()).thenReturn(5000L);
        assertThat(rateLimiter.calculateLimit()).isEqualTo(1);
        when(clock.microTime()).thenReturn(11000L);
        assertThat(rateLimiter.calculateLimit()).isEqualTo(1);
        when(clock.microTime()).thenReturn(20000L);
        assertThat(rateLimiter.calculateLimit()).isEqualTo(2);
    }

    @Test
    public void shouldAdaptToChangedRate() {
        // given
        int secondRate = 2000;
        Clock clock = mock(Clock.class);
        when(clock.currentRate()).thenReturn(secondRate);
        RateLimiter rateLimiter = new RateLimiter(clock, 1);

        // when & then
        when(clock.microTime()).thenReturn(1000L);
        assertThat(rateLimiter.calculateLimit()).isEqualTo(2);
        when(clock.currentRate()).thenReturn(secondRate * 2);
        assertThat(rateLimiter.calculateLimit()).isEqualTo(0);
        when(clock.microTime()).thenReturn(2000L);
        assertThat(rateLimiter.calculateLimit()).isEqualTo(4);
    }
}
