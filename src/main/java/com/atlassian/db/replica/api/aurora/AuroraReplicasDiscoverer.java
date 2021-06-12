package com.atlassian.db.replica.api.aurora;

import com.atlassian.db.replica.internal.aurora.AuroraEndpoint;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import static com.atlassian.db.replica.internal.aurora.AuroraEndpoints.instanceEndpoint;
import static java.util.stream.Collectors.toList;

/**
 * It allows to pull Aurora Replicas cluster information
 */
public final class AuroraReplicasDiscoverer {
    private final Connection connection;

    public AuroraReplicasDiscoverer(Connection connection) {
        this.connection = connection;
    }

    /**
     * It provides list of replica endpoints based on reader endpoint and server ids
     * @param readerEndpoint - Aurora reader endpoint
     * @return list of replicas endpoints
     * @throws SQLException
     */
    public List<AuroraEndpoint> fetchReplicasEndpoints(String readerEndpoint) throws SQLException {
        return fetchReplicasServerIds()
            .stream()
            .map(serverId -> instanceEndpoint(readerEndpoint, serverId))
            .collect(toList());
    }

    /**
     * Asks Aurora about the list of server ids
     * @return server ids
     * @throws SQLException
     */
    public List<String> fetchReplicasServerIds() throws SQLException {
        LinkedList<String> ids = new LinkedList<>();
        try (ResultSet rs =
                 connection.prepareStatement(
                     "SELECT server_id FROM aurora_global_db_instance_status() WHERE session_id != 'MASTER_SESSION_ID'")
                     .executeQuery()) {
            while (rs.next()) {
                ids.add(rs.getString("server_id"));
            }
        }
        return ids;
    }
}

