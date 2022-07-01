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
    private final AuroraJdbcUrl readerUrl;
    private final boolean ignoreInactiveReplicas;

    public AuroraReplicasDiscoverer(AuroraJdbcUrl readerUrl, boolean ignoreInactiveReplicas) {
        this.readerUrl = readerUrl;
        this.ignoreInactiveReplicas = ignoreInactiveReplicas;
    }

    /**
     * Provides jdbc urls for discovered replicas
     *
     * @return list of jdbc urls
     */
    public List<AuroraJdbcUrl> fetchReplicasUrls(Connection connection) throws SQLException {
        return fetchReplicasServerIds(connection)
            .stream()
            .map(serverId ->
                new AuroraJdbcUrl(
                    instanceEndpoint(readerUrl.getEndpoint(), serverId),
                    readerUrl.getDatabaseName()
                )
            )
            .collect(toList());
    }

    private List<String> fetchReplicasServerIds(Connection connection) throws SQLException {
        List<String> ids = new LinkedList<>();
        final String sql = ignoreInactiveReplicas ?
            "SELECT server_id FROM aurora_replica_status() WHERE session_id != 'MASTER_SESSION_ID' and last_update_timestamp > NOW() - INTERVAL '5 minutes'" :
            "SELECT server_id FROM aurora_replica_status() WHERE session_id != 'MASTER_SESSION_ID'";
        try (ResultSet rs =
                 connection.prepareStatement(sql).executeQuery()) {
            while (rs.next()) {
                ids.add(rs.getString("server_id"));
            }
        }
        return ids;
    }
}


