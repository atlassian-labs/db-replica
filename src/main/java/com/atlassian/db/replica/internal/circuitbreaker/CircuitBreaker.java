package com.atlassian.db.replica.internal.circuitbreaker;


public interface CircuitBreaker {
    BreakerState getState();

    void handle(Throwable throwable);
}
