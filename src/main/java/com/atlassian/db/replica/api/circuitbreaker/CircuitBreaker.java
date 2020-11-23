package com.atlassian.db.replica.api.circuitbreaker;

public interface CircuitBreaker {
    BreakerState getState();

    void handle(Throwable throwable);
}
