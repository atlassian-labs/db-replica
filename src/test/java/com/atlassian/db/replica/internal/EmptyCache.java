package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.spi.Cache;

import java.util.Optional;

public class EmptyCache<T> implements Cache<T> {

    @Override
    public Optional<T> get() {
        return Optional.empty();
    }

    @Override
    public void put(T value) {
    }

    @Override
    public void reset() {
    }
}
