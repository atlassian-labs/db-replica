package com.atlassian.db.replica.internal.aurora;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import static com.atlassian.db.replica.internal.aurora.AuroraEndpoints.instanceEndpoint;
import static java.util.stream.Collectors.toList;

/**
 * Allows discovery of Aurora Replicas cluster information
 */
public final class AuroraReplicasDiscoverer {
    private final Connection connection;
    private final AuroraJdbcUrl readerUrl;

    public AuroraReplicasDiscoverer(Connection connection, AuroraJdbcUrl readerUrl) {
        this.connection = connection;
        this.readerUrl = readerUrl;
    }

    /**
     * Provides jdbc url for discovered replicas
     *
     * @return list of jdbc urls
     */
    public List<AuroraJdbcUrl> fetchReplicasUrl() throws SQLException {
        return fetchReplicasServerIds()
            .stream()
            .map(serverId ->
                new AuroraJdbcUrl(
                    instanceEndpoint(readerUrl.getEndpoint(), serverId),
                    readerUrl.getDatabaseName()
                )
            )
            .collect(toList());
    }

    private List<String> fetchReplicasServerIds() throws SQLException {
        LinkedList<String> ids = new LinkedList<>();
        try (ResultSet rs =
                 connection.prepareStatement(
                         "SELECT server_id FROM aurora_replica_status() WHERE session_id != 'MASTER_SESSION_ID'")
                     .executeQuery()) {
            while (rs.next()) {
                ids.add(rs.getString("server_id"));
            }
        }
        return ids;
    }
}


