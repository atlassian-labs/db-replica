package com.atlassian.db.replica.internal;

import java.util.concurrent.atomic.AtomicReference;

public abstract class DecisionAwareReference<T> {
    private final LazyReference<T> lazyReference = new InnerLazyReference<>(this);
    private final AtomicReference<RouteDecisionBuilder> decisionBuilder = new AtomicReference<>();

    public abstract T create() throws Exception;

    public boolean isInitialized() {
        return lazyReference.isInitialized();
    }

    public T get(RouteDecisionBuilder decisionBuilder) {
        if (!isInitialized()) {
            this.decisionBuilder.compareAndSet(null, decisionBuilder);
        }
        return lazyReference.get();
    }

    public void reset() {
        this.lazyReference.reset();
        this.decisionBuilder.set(null);
    }

    public RouteDecisionBuilder getDecisionBuilder() {
        if (this.decisionBuilder.get() == null) {
            throw new IllegalStateException("The decision builder is not initialized");
        }
        return decisionBuilder.get();
    }

    private static final class InnerLazyReference<T> extends LazyReference<T> {

        private final DecisionAwareReference<T> decisionAwareReference;

        private InnerLazyReference(DecisionAwareReference<T> decisionAwareReference) {
            this.decisionAwareReference = decisionAwareReference;
        }

        @Override
        protected T create() throws Exception {
            return decisionAwareReference.create();
        }
    }
}
