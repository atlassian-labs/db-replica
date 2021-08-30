package com.atlassian.db.replica.internal.aurora;

public class ReadReplicaNodeLabelingOperationException extends RuntimeException {

    public ReadReplicaNodeLabelingOperationException(String replicaId, Throwable cause) {
        super("Failed to label a replica connection with replica id: " + replicaId, cause);
    }

}
