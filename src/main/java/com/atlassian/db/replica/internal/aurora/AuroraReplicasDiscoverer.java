package com.atlassian.db.replica.internal.aurora;

import com.atlassian.db.replica.internal.logs.LazyLogger;
import com.atlassian.db.replica.spi.Logger;

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

    private final Logger logger;
    private final LazyLogger lazyLogger;

    public AuroraReplicasDiscoverer(AuroraJdbcUrl readerUrl, Logger logger, LazyLogger lazyLogger) {
        this.readerUrl = readerUrl;
        this.logger = logger;
        this.lazyLogger = lazyLogger;
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
        final String sql = "SELECT server_id, durable_lsn, current_read_lsn, feedback_xmin, " +
            "round(extract(milliseconds from (now()-last_update_timestamp))) as state_lag_in_msec, replica_lag_in_msec " +
            "FROM aurora_replica_status() " +
            "WHERE session_id != 'MASTER_SESSION_ID' and last_update_timestamp > NOW() - INTERVAL '5 minutes';";
        try (ResultSet rs =
                 connection.prepareStatement(sql).executeQuery()) {
            while (rs.next()) {
                String serverId = rs.getString("server_id");
                int replicaLagInMs = rs.getInt("replica_lag_in_msec");
                int durableLsn = rs.getInt("durable_lsn");
                int currentReadLsn = rs.getInt("current_read_lsn");
                int feedbackXmin = rs.getInt("feedback_xmin");
                int stateLag = rs.getInt("state_lag_in_msec");
                logger.debug(String.format(
                    "server_id=%s, replica_lag_in_ms=%d, durable_lsn=%d, current_read_lsn=%d, feedback_xmin=%d, state_lag=%d",
                    serverId,
                    replicaLagInMs,
                    durableLsn,
                    currentReadLsn,
                    feedbackXmin,
                    stateLag
                ));
                lazyLogger.debug(() -> String.format(
                    "server_id=%s, replica_lag_in_ms=%d, durable_lsn=%d, current_read_lsn=%d, feedback_xmin=%d, state_lag=%d",
                    serverId,
                    replicaLagInMs,
                    durableLsn,
                    currentReadLsn,
                    feedbackXmin,
                    stateLag
                ));
                ids.add(serverId);
            }
        }
        return ids;
    }
}


