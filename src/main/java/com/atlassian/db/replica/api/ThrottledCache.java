package com.atlassian.db.replica.api;

import com.atlassian.db.replica.internal.LockBasedThrottledCache;
import com.atlassian.db.replica.internal.util.ThreadSafe;
import com.atlassian.db.replica.spi.SuppliedCache;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;


/**
 * Every read can potentially refresh the cache, however it can be loaded only by one thread at the time.
 * Other threads instead of waiting are getting last known value.
 */
@ThreadSafe
public final class ThrottledCache<T> implements SuppliedCache<T> {
    private final SuppliedCache<T> delegate;

    public ThrottledCache(Clock clock, Duration timeout) {
        this.delegate = new ThrottledCacheWithTimeout<>(clock, timeout);
    }

    /**
     * @deprecated It may be dangerous to use ThrottledCache without any timeout. A single supplier can block the cache refreshes forever.
     * This constructor is available for compatibility only.
     */
    @Deprecated
    public ThrottledCache() {
        this.delegate = new LockBasedThrottledCache<>();
    }

    @Override
    public Optional<T> get(Supplier<T> supplier) {
        return this.delegate.get(supplier);
    }

    @Override
    public Optional<T> get() {
        return this.delegate.get();
    }

    private static class ThrottledCacheWithTimeout<T> implements SuppliedCache<T> {
        private final AtomicReference<Instant> lock = new AtomicReference<>();
        private final Clock clock;
        private final Duration timeout;
        private T value = null;

        private ThrottledCacheWithTimeout(Clock clock, Duration timeout) {
            this.clock = clock;
            this.timeout = timeout;
        }

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
            final boolean hasLock = lock.compareAndSet(null, clock.instant().plus(timeout));
            if (hasLock) {
                loadValue(supplier);
            } else {
                final Instant instant = lock.get();
                final boolean didLockExpire = instant != null && instant.isBefore(clock.instant());
                if (didLockExpire) {
                    final boolean didLockTakeOver = lock.compareAndSet(instant, clock.instant().plus(timeout));
                    if (didLockTakeOver) {
                        loadValue(supplier);
                    }
                }
            }
        }

        private void loadValue(Supplier<T> supplier) {
            try {
                value = supplier.get();
            } finally {
                lock.set(null);
            }
        }
    }
}
