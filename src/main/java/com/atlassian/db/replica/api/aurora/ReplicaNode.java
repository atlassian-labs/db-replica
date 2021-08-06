package com.atlassian.db.replica.api.aurora;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;


//TODO: promote to API?
//TODO: convert to an interface?
public class ReplicaNode {
    private static final String AURORA_REPLICA_ID = "replicaId";

    public Connection mark(final Connection connection) { //TODO: Why inet_server_addr? It's not compatible with auroraCluster#database#uuid
        try (ResultSet rs = connection.prepareStatement("SELECT inet_server_addr() as ip;").executeQuery()) {
            if (rs.next()) {
                String server = rs.getString("ip");
                connection.getClientInfo().setProperty(AURORA_REPLICA_ID, server);
            }
        } catch (Exception e) {
//            log.withoutCustomerData().error("Failed to label a connection", e);
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
