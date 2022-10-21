package com.atlassian.db.replica.internal.aurora;

final class ReadReplicaDiscovererCreationException extends RuntimeException {

    ReadReplicaDiscovererCreationException(Throwable cause) {
        super("Failed to create AuroraReplicasDiscoverer", cause);
    }
}
