package com.atlassian.db.replica.internal;

import java.util.concurrent.atomic.AtomicReference;

public abstract class DecisionAwareReference<T> extends LazyReference<T> {
    private final AtomicReference<RouteDecisionBuilder> firstCause = new AtomicReference<>();

    public DecisionAwareReference() {
        super();
    }

    public T get(RouteDecisionBuilder currentCause) {
        firstCause.compareAndSet(null, currentCause);
        return super.get();
    }

    @Override
    public void reset() {
        super.reset();
        firstCause.set(null);
    }

    public void set(T reference) {
        super.set(reference);
        firstCause.set(null);
    }

    public RouteDecisionBuilder getFirstCause() {
        if (firstCause.get() == null) {
            throw new IllegalStateException("The decision builder is not initialized");
        }
        return firstCause.get();
    }
}
