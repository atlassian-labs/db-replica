package com.atlassian.db.replica.impl;

import com.atlassian.db.replica.spi.*;

import java.util.*;

public class VolatileCache<T> implements Cache<T> {

    private volatile T value;

    @Override
    public Optional<T> get() {
        return Optional.ofNullable(value);
    }

    @Override
    public void put(T value) {
        this.value = value;
    }

    @Override
    public void reset() {
        value = null;
    }
}
