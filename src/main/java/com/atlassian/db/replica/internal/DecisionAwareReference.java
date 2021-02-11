package com.atlassian.db.replica.internal;

import java.util.concurrent.atomic.AtomicReference;

public abstract class DecisionAwareReference<T> extends LazyReference<T> {
    private final AtomicReference<RouteDecisionBuilder> decisionBuilder = new AtomicReference<>();

    public T get(RouteDecisionBuilder decisionBuilder) {
        if (!isInitialized()) {
            this.decisionBuilder.compareAndSet(null, decisionBuilder);
        }
        return super.get();
    }

    @Override
    public void reset() {
        super.reset();
        this.decisionBuilder.set(null);
    }

    public RouteDecisionBuilder getDecisionBuilder() {
        if (this.decisionBuilder.get() == null) {
            throw new IllegalStateException("The decision builder is not initialized");
        }
        return decisionBuilder.get();
    }
}
