package com.atlassian.db.replica.internal.aurora;

public class ReadReplicaConnectionCreationException extends RuntimeException {

    public ReadReplicaConnectionCreationException(Throwable cause) {
        super("Failure during replica connection creation", cause);
    }
}
