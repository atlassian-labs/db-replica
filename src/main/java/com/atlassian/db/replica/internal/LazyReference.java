package com.atlassian.db.replica.internal;


import com.atlassian.db.replica.internal.util.ThreadSafe;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

@ThreadSafe
public abstract class LazyReference<T> implements Supplier<T> {
    private T reference;
    private final AtomicReference<T> atomicReference = new AtomicReference<>();
    private final Object lock = new Object();
    private final boolean compatibleWithPreviousVersion;

    protected LazyReference(boolean compatibleWithPreviousVersion) {
        this.compatibleWithPreviousVersion = compatibleWithPreviousVersion;
    }

    protected abstract T create() throws Exception;

    public boolean isInitialized() {
        return compatibleWithPreviousVersion ? isInitialized_old() : isInitialized_new();
    }

    @Override
    public T get() {
        return compatibleWithPreviousVersion ? get_old() : get_new();
    }

    public void reset() {
        if (compatibleWithPreviousVersion) {
            reset_old();
        } else {
            reset_new();
        }
    }

    private boolean isInitialized_old() {
        return atomicReference.get() != null;
    }

    private T get_old() {
        if (!isInitialized()) {
            try {
                atomicReference.compareAndSet(null, create());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return atomicReference.get();
    }

    private void reset_old() {
        atomicReference.set(null);
    }

    private T get_new() {
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

    private boolean isInitialized_new() {
        return reference != null;
    }

    private void reset_new() {
        reference = null;
    }
}
