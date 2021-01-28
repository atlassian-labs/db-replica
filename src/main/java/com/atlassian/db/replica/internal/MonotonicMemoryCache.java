package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.spi.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static com.atlassian.db.replica.internal.util.Comparables.*;

/**
 * Holds values that grow over time, unless reset. Holds a value in JVM memory.
 *
 * @param <T> type of the cached value
 */
public class MonotonicMemoryCache<T extends Comparable<T>> implements Cache<T> {

    private final AtomicReference<T> cache = new AtomicReference<>(null);

    @Override
    public Optional<T> get() {
        return Optional.ofNullable(cache.get());
    }

    @Override
    public void put(T value) {
        cache.updateAndGet(prev -> max(prev, value));
    }

    @Override
    public void reset() {
        cache.set(null);
    }
}
