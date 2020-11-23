package com.atlassian.db.replica.api.circuitbreaker;

public enum BreakerState {
    OPEN,
    HALF_CLOSED,
    CLOSED
}
