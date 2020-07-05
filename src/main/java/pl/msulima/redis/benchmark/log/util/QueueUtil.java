package pl.msulima.redis.benchmark.log.util;

import java.util.Queue;

public final class QueueUtil {

    private QueueUtil() {
    }

    public static <T> void offerOrSpin(Queue<T> commandQueue, T runnable) {
        while (!commandQueue.offer(runnable)) {
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
            Thread.yield();
        }
    }
}
