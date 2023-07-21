package com.atlassian.db.replica.it.example.aurora;

import com.atlassian.db.replica.internal.NotLoggingLogger;
import com.atlassian.db.replica.internal.aurora.AuroraEndpoint;
import com.atlassian.db.replica.internal.aurora.AuroraJdbcUrl;
import com.atlassian.db.replica.internal.aurora.AuroraReplicasDiscoverer;
import com.atlassian.db.replica.internal.aurora.ReadReplicaDiscovererCreationException;
import com.atlassian.db.replica.it.example.aurora.replica.AuroraConnectionProvider;
import com.atlassian.db.replica.spi.ConnectionProvider;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AuroraClusterTest {
    final String databaseName = "postgres";
    final String jdbcPassword = System.getenv("password");

    @Test
    void should_discover_replicas_on_aurora_1_3_6() throws SQLException {
        final String readerEndpoint = "database-2.cluster-ro-c2vqiwlre1di.eu-central-1.rds.amazonaws.com:5432";
        final String readerJdbcUrl = "jdbc:postgresql://" + readerEndpoint + "/" + databaseName;
        final String writerJdbcUrl = "jdbc:postgresql://database-2.cluster-c2vqiwlre1di.eu-central-1.rds.amazonaws.com:5432" + "/" + databaseName;

        final ConnectionProvider connectionProvider = getConnectionProvider(readerJdbcUrl, writerJdbcUrl, jdbcPassword);

        final int visibleReplicasMainDbPerspective = getVisibleReplicas(connectionProvider.getMainConnection());
        final int visibleReplicasReplicaDbPerspective = getVisibleReplicas(connectionProvider.getReplicaConnection());

        assertThat(visibleReplicasMainDbPerspective).isEqualTo(1);
        assertThat(visibleReplicasReplicaDbPerspective).isEqualTo(1);
    }


    @Test
    void should_discover_replicas_on_aurora_1_3_9() throws SQLException {
        final String readerEndpoint = "database-3.cluster-ro-c2vqiwlre1di.eu-central-1.rds.amazonaws.com:5432";
        final String readerJdbcUrl = "jdbc:postgresql://" + readerEndpoint + "/" + databaseName;
        final String writerJdbcUrl = "jdbc:postgresql://database-3.cluster-c2vqiwlre1di.eu-central-1.rds.amazonaws.com:5432" + "/" + databaseName;

        final ConnectionProvider connectionProvider = getConnectionProvider(readerJdbcUrl, writerJdbcUrl, jdbcPassword);

        final int visibleReplicasMainDbPerspective = getVisibleReplicas(connectionProvider.getMainConnection());
        final int visibleReplicasReplicaDbPerspective = getVisibleReplicas(connectionProvider.getReplicaConnection());

        assertThat(visibleReplicasMainDbPerspective).isEqualTo(1);
        assertThat(visibleReplicasReplicaDbPerspective).isEqualTo(1);
    }

    private int getVisibleReplicas(Connection connectionProvider) throws SQLException {
        try (final Connection connection = connectionProvider) {
            final AuroraReplicasDiscoverer discoverer = createDiscovererFromConnection(connection);
            final List<AuroraJdbcUrl> auroraJdbcUrls = discoverer.fetchReplicasUrls(connection);
            return auroraJdbcUrls.size();
        }
    }

    private ConnectionProvider getConnectionProvider(
        String readerJdbcUrl, String writerJdbcUrl, String password
    ) throws SQLException {
        return new AuroraConnectionProvider(readerJdbcUrl, writerJdbcUrl, password);
    }

    private AuroraReplicasDiscoverer createDiscovererFromConnection(Connection connection) {
        try {
            final String databaseUrl = connection.getMetaData().getURL();
            final String[] split = databaseUrl.split("/");
            final String readerEndpoint = split[2];
            final String databaseName = split[3];
            return new AuroraReplicasDiscoverer(
                new AuroraJdbcUrl(AuroraEndpoint.parse(readerEndpoint), databaseName),
                new NotLoggingLogger());
        } catch (SQLException exception) {
            throw new ReadReplicaDiscovererCreationException(exception);
        }
    }
}
