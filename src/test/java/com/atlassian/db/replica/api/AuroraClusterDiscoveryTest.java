package com.atlassian.db.replica.api;

import com.atlassian.db.replica.internal.aurora.AuroraClusterDiscovery;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

class AuroraClusterDiscoveryTest {
    final String readerEndpoint = "database-1.cluster-xxxxxxxxxxxxx.xx-xxxxxx-1.rds.amazonaws.com:5432";
    final String dbName = "dbname";

    @Test
    void shouldDiscoverReplicaConnectionsWhenScaledUp() throws SQLException {
        final AuroraClusterMock auroraClusterMock = new AuroraClusterMock();
        auroraClusterMock
            .scaleUp()
            .scaleUp();
        final AuroraClusterDiscovery auroraCluster = new AuroraClusterDiscovery(
            readerEndpoint,
            dbName
        );

        Assertions.assertThat(auroraCluster.getReplicas(auroraClusterMock::getMainConnection).size()).isEqualTo(2);
    }

    @Test
    void shouldDiscoverReplicaConnectionsWhenScaledDown() throws SQLException {
        final AuroraClusterMock auroraClusterMock = new AuroraClusterMock();
        auroraClusterMock
            .scaleUp()
            .scaleDown()
            .scaleUp();
        final AuroraClusterDiscovery auroraCluster = new AuroraClusterDiscovery(
            readerEndpoint,
            dbName
        );

        Assertions.assertThat(auroraCluster.getReplicas(auroraClusterMock::getMainConnection).size()).isEqualTo(1);
    }
}
