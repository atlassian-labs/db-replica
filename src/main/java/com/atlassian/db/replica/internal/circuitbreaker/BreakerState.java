package com.atlassian.db.replica.internal.circuitbreaker;

/**
 * States of <a href="https://www.martinfowler.com/bliki/CircuitBreaker.html">circuit breakers</a>.
 */
public enum BreakerState {
    OPEN,
    HALF_CLOSED,
    CLOSED
}
