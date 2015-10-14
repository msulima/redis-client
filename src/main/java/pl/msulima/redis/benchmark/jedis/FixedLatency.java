package pl.msulima.redis.benchmark.jedis;

public class FixedLatency {

    public static final int FIXED_LATENCY = 5;

    public static void fixedLatency() {
        try {
            //noinspection ConstantConditions
            if (FIXED_LATENCY > 0) {
                Thread.sleep(FIXED_LATENCY);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
