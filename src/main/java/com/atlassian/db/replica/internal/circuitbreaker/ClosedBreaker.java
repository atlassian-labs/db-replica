package com.atlassian.db.replica.internal.circuitbreaker;

public class ClosedBreaker implements CircuitBreaker {
    @Override
    public BreakerState getState() {
        return BreakerState.CLOSED;
    }

    @Override
    public void handle(Throwable throwable) {

    }
}
