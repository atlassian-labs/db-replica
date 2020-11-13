package com.atlassian.db.replica.internal;

import java.util.function.BiFunction;

class Operation<S,T> {
    private final BiFunction<S, Args<T>, Void> operation;
    private final Args<T> args;

    public Operation(BiFunction<S, Args<T>, Void> operation, Args<T> args) {
        this.operation = operation;
        this.args = args;
    }

    public void run(final S statement) {
        operation.apply(statement, args);
    }
}
