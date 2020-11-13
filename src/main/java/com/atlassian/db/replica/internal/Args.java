package com.atlassian.db.replica.internal;

class Args<T> {
    private final int parameterIndex;
    private final T value;

    Args(int parameterIndex, T value) {
        this.parameterIndex = parameterIndex;
        this.value = value;
    }

    public int getParameterIndex() {
        return parameterIndex;
    }

    public T getValue() {
        return value;
    }
}
