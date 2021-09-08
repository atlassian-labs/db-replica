package com.atlassian.db.replica.internal.aurora;

import com.atlassian.db.replica.spi.ReplicaConnectionProvider;
import com.atlassian.db.replica.api.Database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

public class AuroraReplicaNode implements Database {
    private final String id;
    private final ReplicaConnectionProvider replicaConnectionProvider;

    public AuroraReplicaNode(
        String id,
        ReplicaConnectionProvider replicaConnectionProvider
    ) {
        this.id = id;
        this.replicaConnectionProvider = replicaConnectionProvider;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Supplier<Connection> getConnectionSupplier() {
        return () -> {
            try {
                return replicaConnectionProvider.getReplicaConnection();
            } catch (SQLException exception) {
                throw new ReadReplicaConnectionCreationException(exception);
            }
        };
    }
}
