package com.atlassian.db.replica.internal.circuitbreaker;

import com.atlassian.db.replica.internal.ReadReplicaUnsupportedOperationException;

import java.sql.SQLFeatureNotSupportedException;

import static com.atlassian.db.replica.internal.circuitbreaker.BreakerState.CLOSED;
import static com.atlassian.db.replica.internal.circuitbreaker.BreakerState.OPEN;

public class BreakOnNotSupportedOperations implements CircuitBreaker {
    private static volatile BreakerState state = CLOSED;

    @Override
    public BreakerState getState() {
        return state;
    }

    @Override
    public void handle(Throwable throwable) {
        if (throwable instanceof ReadReplicaUnsupportedOperationException || throwable instanceof SQLFeatureNotSupportedException) {
            state = OPEN;
        }
    }

    public static void reset() {
        state = CLOSED;
    }
}
