package com.atlassian.db.replica.internal.aurora;

public class ReadReplicaDiscoveryOperationException extends RuntimeException {

    public ReadReplicaDiscoveryOperationException(Throwable cause) {
        super("Failure during read replicas discovery operation", cause);
    }
}
