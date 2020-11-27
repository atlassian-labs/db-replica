package com.atlassian.db.replica.internal.circuitbreaker;

import com.atlassian.db.replica.api.circuitbreaker.BreakerState;
import com.atlassian.db.replica.spi.circuitbreaker.CircuitBreaker;

public class ClosedBreaker implements CircuitBreaker {
    @Override
    public BreakerState getState() {
        return BreakerState.CLOSED;
    }

    @Override
    public void handle(Throwable throwable) {

    }
}
