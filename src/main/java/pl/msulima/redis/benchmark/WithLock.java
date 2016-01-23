package pl.msulima.redis.benchmark;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class WithLock {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public <T> T reading(Supplier<T> function) {
        try {
            lock.readLock().lock();
            return function.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    public <T> T writing(Supplier<T> function) {
        try {
            lock.writeLock().lock();
            return function.get();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
