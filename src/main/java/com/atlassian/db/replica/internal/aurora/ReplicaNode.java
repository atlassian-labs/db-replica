package com.atlassian.db.replica.internal.aurora;

import java.sql.Connection;
import java.sql.SQLException;


public class ReplicaNode {
    private static final String AURORA_REPLICA_ID = "replicaId";

    public Connection mark(final Connection connection, final String replicaId) {
        try {
            connection.getClientInfo().setProperty(AURORA_REPLICA_ID, replicaId);
        } catch (SQLException exception) {
            throw new ReadReplicaNodeLabelingOperationException(replicaId, exception);
        }
        return connection;
    }

    public String get(Connection replica) {
        try {
            return replica.getClientInfo(AURORA_REPLICA_ID);
        } catch (SQLException e) {
//            LOG.withCustomerData().error("Failed to fetch aurora server id", e);
            return null;
        }
    }
}
