package com.atlassian.db.replica.api.exception;

public final class ConnectionCouldNotBeClosedException extends RuntimeException {
    public ConnectionCouldNotBeClosedException(Throwable cause) {
        super(cause);
    }
}
