package com.atlassian.db.replica.internal.aurora;

public class ReadReplicaDiscovererCreationException extends RuntimeException {

    public ReadReplicaDiscovererCreationException(Throwable cause) {
        super("Failed to create AuroraReplicasDiscoverer", cause);
    }
}
