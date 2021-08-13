package com.atlassian.db.replica.it.example.aurora.replica.api;

import com.atlassian.db.replica.spi.ConnectionProvider;

import java.sql.Connection;
import java.sql.SQLException;

//TODO promote to the API or hide from end user?
public class ReplicaNodeAwareConnectionProvider implements ConnectionProvider {
    private final ConnectionProvider connectionProvider;

    public ReplicaNodeAwareConnectionProvider(
        ConnectionProvider connectionProvider
    ) {
        this.connectionProvider = connectionProvider;
    }

    @Override
    public boolean isReplicaAvailable() {
        return connectionProvider.isReplicaAvailable();
    }

    @Override
    public Connection getMainConnection() throws SQLException {
        return connectionProvider.getMainConnection();
    }

    @Override
    public Connection getReplicaConnection() throws SQLException {
        return connectionProvider.getReplicaConnection();
    }
}
