package com.atlassian.db.replica.spi;

import com.atlassian.db.replica.internal.util.cache.MonotonicMemoryCache;

import java.util.*;

/**
 * Holds a single value. Might be empty.
 *
 * @param <T> cached value
 */
public interface Cache<T> {

    static <C extends Comparable<C>> Cache<C> cacheMonotonicValuesInMemory() {
        return new MonotonicMemoryCache<>();
    }

    /**
     * @return last known value or empty if it's unknown, not null
     */
    Optional<T> get();

    /**
     * @param value last known value
     */
    void put(T value);

    /**
     * Forgets the last known value.
     */
    void reset();
}
