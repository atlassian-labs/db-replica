package com.atlassian.db.replica.api.circuitbreaker;

public class DualConnectionException extends RuntimeException {
    public DualConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
