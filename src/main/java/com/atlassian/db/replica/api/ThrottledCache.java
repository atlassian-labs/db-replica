package com.atlassian.db.replica.api;

import com.atlassian.db.replica.internal.util.ThreadSafe;
import com.atlassian.db.replica.spi.SuppliedCache;

import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;


/**
 * Every read can potentially refresh the cache, however it can be loaded only by one thread at the time.
 * Other threads instead of waiting are getting last known value.
 */
@ThreadSafe
public class ThrottledCache<T> implements SuppliedCache<T> {
    private final ReentrantLock lock = new ReentrantLock();
    private T value = null;

    @Override
    public Optional<T> get(Supplier<T> supplier) {
        maybeRefresh(supplier);
        return Optional.ofNullable(value);
    }

    private void maybeRefresh(Supplier<T> supplier) {
        if(lock.tryLock()) {
            try {
                value = supplier.get();
            } finally {
                lock.unlock();
            }
        }
    }
}
