package com.atlassian.db.replica.internal;

/**
 * I just want to have a single place to keep debugger aware of calls to unsupported operations.
 * I plan to remove it once all the calls supported.
 */
public class ReadReplicaUnsupportedOperationException extends RuntimeException {
    public ReadReplicaUnsupportedOperationException() {
        super();
    }

    public ReadReplicaUnsupportedOperationException(Throwable cause) {
        super(cause);
    }
}
