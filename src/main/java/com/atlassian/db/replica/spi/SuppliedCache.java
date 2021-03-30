package com.atlassian.db.replica.spi;

import com.atlassian.db.replica.internal.util.ThreadSafe;

import java.util.Optional;
import java.util.function.Supplier;

@ThreadSafe
public interface SuppliedCache<T> {
    /**
     * @param supplier used to populate the value. It must always return a value, never null.
     * @return T last remembered value or empty
     */
    Optional<T> get(Supplier<T> supplier);
}
