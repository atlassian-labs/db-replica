package com.atlassian.db.replica.internal.aurora;

import com.atlassian.db.replica.spi.DataSource;
import com.atlassian.db.replica.spi.ReplicaConnectionProvider;
import com.atlassian.db.replica.api.Database;

import java.sql.SQLException;
import java.util.Optional;

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
    public Optional<String> getId() {
        return Optional.ofNullable(id);
    }

    @Override
    public DataSource getDataSource() {
        return () -> {
            try {
                return replicaConnectionProvider.getReplicaConnection();
            } catch (SQLException exception) {
                throw new ReadReplicaConnectionCreationException(exception);
            }
        };
    }
}
