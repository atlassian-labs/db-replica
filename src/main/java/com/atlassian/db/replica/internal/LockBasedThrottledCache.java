package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.internal.util.ThreadSafe;
import com.atlassian.db.replica.spi.SuppliedCache;

import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

@ThreadSafe
public final class LockBasedThrottledCache<T> implements SuppliedCache<T> {
    private final ReentrantLock lock = new ReentrantLock();
    private T value = null;

    @Override
    public Optional<T> get(Supplier<T> supplier) {
        maybeRefresh(supplier);
        return Optional.ofNullable(value);
    }

    @Override
    public Optional<T> get() {
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
