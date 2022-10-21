package com.atlassian.db.replica.internal.aurora;

final class ReadReplicaDiscoveryOperationException extends RuntimeException {

    ReadReplicaDiscoveryOperationException(Throwable cause) {
        super("Failure during read replicas discovery operation", cause);
    }
}
