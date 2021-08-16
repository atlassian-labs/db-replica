package com.atlassian.db.replica.internal;


import com.atlassian.db.replica.internal.util.ThreadSafe;

import java.util.function.Supplier;

@ThreadSafe
public abstract class LazyReference<T> implements Supplier<T> {
    private T reference;
    private final Object lock = new Object();

    protected LazyReference() {
    }

    protected abstract T create() throws Exception;

    public boolean isInitialized() {
        return reference != null;
    }

    @Override
    public T get() {
        synchronized (lock) {
            if (!isInitialized()) {
                try {
                    reference = create();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return reference;
    }

    protected void set(T reference) {
        synchronized (lock) {
            this.reference = reference;
        }
    }

    public void reset() {
        reference = null;
    }

}
