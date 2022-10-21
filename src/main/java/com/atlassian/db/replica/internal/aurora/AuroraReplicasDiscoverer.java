package com.atlassian.db.replica.internal.aurora;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import static com.atlassian.db.replica.internal.aurora.AuroraCluster.AuroraClusterBuilder.anAuroraCluster;
import static com.atlassian.db.replica.internal.aurora.AuroraEndpoint.AuroraEndpointBuilder.anAuroraEndpoint;
import static java.util.stream.Collectors.toList;

/**
 * Allows discovery of Aurora Replicas cluster information
 */
final class AuroraReplicasDiscoverer {
    private final AuroraJdbcUrl readerUrl;

    AuroraReplicasDiscoverer(AuroraJdbcUrl readerUrl) {
        this.readerUrl = readerUrl;
    }

    /**
     * Provides jdbc urls for discovered replicas
     *
     * @return list of jdbc urls
     */
    List<AuroraJdbcUrl> fetchReplicasUrls(Connection connection) throws SQLException {
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

    /**
     * Transforms reader endpoint to instance endpoint
     */
    private AuroraEndpoint instanceEndpoint(AuroraEndpoint readerEndpoint, String serverId) {
        return anAuroraEndpoint(readerEndpoint)
            .serverId(serverId)
            .cluster(anAuroraCluster(readerEndpoint.getCluster().getClusterName()).clusterPrefix(null).build())
            .build();
    }

    private List<String> fetchReplicasServerIds(Connection connection) throws SQLException {
        List<String> ids = new LinkedList<>();
        final String sql = "SELECT server_id FROM aurora_replica_status() WHERE session_id != 'MASTER_SESSION_ID' and last_update_timestamp > NOW() - INTERVAL '5 minutes'";
        try (ResultSet rs =
                 connection.prepareStatement(sql).executeQuery()) {
            while (rs.next()) {
                ids.add(rs.getString("server_id"));
            }
        }
        return ids;
    }
}


