package com.atlassian.db.replica.it.example.aurora.replica.api;

import com.atlassian.db.replica.api.aurora.ReplicaNode;
import com.atlassian.db.replica.spi.ConnectionProvider;

import java.sql.Connection;
import java.sql.SQLException;

//TODO promote to the API or hide from end user?
public class ReplicaNodeAwareConnectionProvider implements ConnectionProvider {
    private final ConnectionProvider connectionProvider;
    private final ReplicaNode replicaNode;

    public ReplicaNodeAwareConnectionProvider(
        ConnectionProvider connectionProvider,
        ReplicaNode replicaNode
    ) {
        this.connectionProvider = connectionProvider;
        this.replicaNode = replicaNode;
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
        final Connection replicaConnection = connectionProvider.getReplicaConnection();
        replicaNode.mark(replicaConnection);
        return replicaConnection;
    }
}
