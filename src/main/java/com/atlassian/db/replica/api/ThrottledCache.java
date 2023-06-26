package com.atlassian.db.replica.api;

import com.atlassian.db.replica.internal.LockBasedThrottledCache;
import com.atlassian.db.replica.internal.util.ThreadSafe;
import com.atlassian.db.replica.spi.SuppliedCache;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.time.Duration.ofSeconds;
import static java.time.Instant.now;


/**
 * Every read can potentially refresh the cache, however it can be loaded only by one thread at the time.
 * Other threads instead of waiting are getting last known value. When loading the value takes longer
 * than `timeout`, a new thread is allowed to start loading the value. Results of the previous load will be
 * discarded.
 * <p>
 * Results of the timed-out load are not discarded when whe know the cached values are always increasing
 * (provide a comparator {@link Builder#sequenceCache(Comparator)} to enable this feature).
 */
@ThreadSafe
public final class ThrottledCache<T> implements SuppliedCache<T> {
    private final SuppliedCache<T> delegate;

    public static <T> ThrottledCache.Builder<T> builder(Clock clock, Duration timeout) {
        return new Builder<>(clock, timeout);
    }

    /**
     * @deprecated use `ThrottledCache#builder`
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
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

    private ThrottledCache(Clock clock, Duration timeout, Comparator<T> comparator) {
        this.delegate = new ThrottledSequenceCacheWithTimeout<>(clock, timeout, comparator);
    }

    @Override
    public Optional<T> get(Supplier<T> supplier) {
        return this.delegate.get(supplier);
    }

    @Override
    public Optional<T> get() {
        return this.delegate.get();
    }

    public static class Builder<T> {
        private final Clock clock;
        private final Duration timeout;
        private Comparator<T> comparator;

        private Builder(Clock clock, Duration timeout) {
            this.clock = clock;
            this.timeout = timeout;
        }

        /**
         * Ensures the cached value can be only incremented
         */
        public Builder<T> sequenceCache(Comparator<T> comparator) {
            this.comparator = comparator;
            return this;
        }

        public ThrottledCache<T> build() {
            return comparator == null ? new ThrottledCache<>(clock, timeout) :
                new ThrottledCache<>(
                    clock,
                    timeout,
                    comparator
                );
        }
    }

    private static class ThrottledSequenceCacheWithTimeout<T> implements SuppliedCache<T> {
        private final Clock clock;
        private final Duration timeout;
        private final Comparator<T> comparator;
        private final AtomicReference<ThrottledCache.LockValuePair<T>> lockValuePair = new AtomicReference<>(
            new ThrottledCache.LockValuePair<>(
                null,
                null
            )
        );

        private ThrottledSequenceCacheWithTimeout(Clock clock, Duration timeout, Comparator<T> comparator) {
            this.clock = clock;
            this.timeout = timeout;
            this.comparator = comparator;
        }

        @Override
        public Optional<T> get(Supplier<T> supplier) {
            maybeRefresh(supplier);
            return get();
        }

        @Override
        public Optional<T> get() {
            return Optional.ofNullable(lockValuePair.get().getValue());
        }

        private void maybeRefresh(Supplier<T> supplier) {
            final LockValuePair<T> currentLock = this.lockValuePair.get();
            if (currentLock.isLocked()) {
                if (currentLock.didLockExpire(clock)) {
                    tryLock().ifPresent(lock -> loadValue(supplier, lock));
                }
            } else {
                tryLock().ifPresent(lock -> loadValue(supplier, lock));
            }
        }

        private Optional<ThrottledCache.LockValuePair<T>> tryLock() {
            final LockValuePair<T> currentLockValuePair = this.lockValuePair.get();
            final LockValuePair<T> newLock = new LockValuePair<>(
                newDeadLine(),
                currentLockValuePair.getValue()
            );
            return this.lockValuePair.compareAndSet(
                currentLockValuePair,
                newLock
            ) ? Optional.of(newLock) : Optional.empty();
        }

        private Instant newDeadLine() {
            return clock.instant().plus(this.timeout);
        }

        private void loadValue(Supplier<T> supplier, LockValuePair<T> currentLockValue) {
            final T newValue = tryLoad(supplier, currentLockValue);
            if (!isIncreasing(currentLockValue, newValue)) {
                releaseLock(currentLockValue);
                return; // Sequence can only grow, so the value is older than already cached
            }
            if (releaseLock(currentLockValue, newValue)) {
                return; // The value was successfully loaded and the lock was released.
            }
            tryUpdate(newValue);
        }

        /***
         * Current thread no longer have a lock, but may have a newer value to update the cache. We'll spin-lock
         * until the newest value is loaded into the cache. Times out silently after 5 seconds (I'd expect it will
         * happen only for long GC pauses, as the lock contention should be low).
         */
        private void tryUpdate(T newValue) {
            final Instant deadline = now().plus(ofSeconds(5));
            do {
                final LockValuePair<T> lock = this.lockValuePair.get();
                if (isIncreasing(lock, newValue)) { // Another thread updated the value, but we have a newer value
                    updateValue(lock, newValue);
                } else {
                    break;
                }
            } while (now().isBefore(deadline));
        }

        private boolean isIncreasing(LockValuePair<T> currentLockValue, T newValue) {
            return currentLockValue.getValue() == null || comparator.compare(
                newValue,
                currentLockValue.getValue()
            ) > 0;
        }

        private T tryLoad(Supplier<T> supplier, LockValuePair<T> currentLockValue) {
            final T newValue;
            try {
                newValue = supplier.get();
            } catch (Exception e) {
                releaseLock(currentLockValue);
                throw e;
            }
            return newValue;
        }

        /**
         * Updates value, but keeps the deadline.
         */
        private boolean updateValue(LockValuePair<T> currentLockValue, T newValue) {
            return this.lockValuePair.compareAndSet(
                currentLockValue,
                new LockValuePair<>(
                    currentLockValue.timeout,
                    newValue
                )
            );
        }

        /**
         * Unlocks the cache and sets a new value.
         */
        private boolean releaseLock(LockValuePair<T> currentLockValue, T newValue) {
            return this.lockValuePair.compareAndSet(
                currentLockValue,
                new LockValuePair<>(
                    null,
                    newValue
                )
            );
        }

        /**
         * Unlocks the cache and keeps the old value.
         */
        private void releaseLock(LockValuePair<T> currentLockValue) {
            releaseLock(currentLockValue, currentLockValue.getValue());
        }
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
            return get();
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
