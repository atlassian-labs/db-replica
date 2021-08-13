package com.atlassian.db.replica.api.aurora;

import java.sql.Connection;
import java.sql.SQLException;


//TODO: promote to API?
//TODO: convert to an interface?
public class ReplicaNode {
    private static final String AURORA_REPLICA_ID = "replicaId";

    public Connection mark(final Connection connection, final String repliacId)  {
        try {
            connection.getClientInfo().setProperty(AURORA_REPLICA_ID, repliacId);
        } catch (SQLException throwables) {
            //TODO:            log.withoutCustomerData().error("Failed to label a connection", e);

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
