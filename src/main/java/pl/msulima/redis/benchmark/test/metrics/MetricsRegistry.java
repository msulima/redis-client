package pl.msulima.redis.benchmark.test.metrics;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.concurrent.TimeUnit;

public class MetricsRegistry {

    private static final long HIGHEST_TRACKABLE_VALUE = 10_000_000L;
    private static final int NUMBER_OF_SIGNIFICANT_VALUE_DIGITS = 4;

    private final Recorder recorder = new Recorder(HIGHEST_TRACKABLE_VALUE, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);
    private final ConcurrentHistogram histogram = new ConcurrentHistogram(HIGHEST_TRACKABLE_VALUE, NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);

    public void recordTime(long value, TimeUnit unit, int count) {
        recorder.recordValueWithCount(unit.toMicros(value), count);
    }

    public Histogram histogramSnapshot() {
        histogram.add(recorder.getIntervalHistogram());
        return histogram.copy();
    }
}
