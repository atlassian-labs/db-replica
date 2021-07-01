package com.atlassian.db.replica.api;

import com.atlassian.db.replica.internal.LockBasedThrottledCache;
import com.atlassian.db.replica.internal.util.ThreadSafe;
import com.atlassian.db.replica.spi.SuppliedCache;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
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
        private final Clock clock;
        private final Duration timeout;
        private final AtomicReference<LockValuePair<T>> lockValuePair = new AtomicReference<>(
            new LockValuePair<>(
                null,
                null
            )
        );

        private ThrottledCacheWithTimeout(Clock clock, Duration timeout) {
            this.clock = clock;
            this.timeout = timeout;
        }

        @Override
        public Optional<T> get(Supplier<T> supplier) {
            maybeRefresh(supplier);
            return Optional.ofNullable(lockValuePair.get().getValue());
        }

        @Override
        public Optional<T> get() {
            return Optional.ofNullable(lockValuePair.get().getValue());
        }

        private void maybeRefresh(Supplier<T> supplier) {
            final Instant deadline = clock.instant().plus(this.timeout);
            final T currentValue = this.lockValuePair.get().getValue();
            final LockValuePair<T> valueWithLock = new LockValuePair<>(
                deadline,
                currentValue
            );
            final LockValuePair<T> currentLockValuePair = this.lockValuePair.get();
            final boolean hasLock = !currentLockValuePair.isLocked() && this.lockValuePair.compareAndSet(
                currentLockValuePair,
                valueWithLock
            );
            if (hasLock) {
                loadValue(supplier, valueWithLock);
            } else {
                final LockValuePair<T> oldLock = this.lockValuePair.get();
                if (oldLock.didLockExpire(clock)) {
                    final LockValuePair<T> newLock = new LockValuePair<>(
                        deadline,
                        oldLock.getValue()
                    );
                    final boolean didLockTakeOver = lockValuePair
                        .compareAndSet(oldLock, newLock);
                    if (didLockTakeOver) {
                        loadValue(supplier, newLock);
                    }
                }
            }
        }

        private void loadValue(Supplier<T> supplier, LockValuePair<T> currentLockValue) {
            try {
                final T newValue = supplier.get();
                this.lockValuePair.compareAndSet(
                    currentLockValue,
                    new LockValuePair<>(
                        null,
                        newValue
                    )
                );
            } catch (Exception e) {
                this.lockValuePair.compareAndSet(
                    currentLockValue,
                    new LockValuePair<>(
                        null,
                        currentLockValue.getValue()
                    )
                );
                throw e;
            }
        }

        private static final class LockValuePair<T> {
            private final Instant timeout;
            private final T value;

            private LockValuePair(Instant timeout, T value) {
                this.timeout = timeout;
                this.value = value;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                LockValuePair<?> that = (LockValuePair<?>) o;
                return Objects.equals(timeout, that.timeout) && Objects.equals(value, that.value);
            }

            public T getValue() {
                return value;
            }

            public boolean didLockExpire(Clock clock) {
                return timeout != null && timeout.isBefore(clock.instant());
            }

            public boolean isLocked() {
                return timeout != null;
            }

            @Override
            public int hashCode() {
                return Objects.hash(timeout, value);
            }

            @Override
            public String toString() {
                return "ValueWithLock{" +
                    "timeout=" + timeout +
                    ", value=" + value +
                    '}';
            }
        }
    }
}
